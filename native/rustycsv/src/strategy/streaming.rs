// Approach D: Streaming Parser
//
// Stateful chunked parser for processing large files with bounded memory.
// Feed chunks of data and extract complete rows as they become available.
//
// Key design:
// - Owns data (Vec<u8>) because input chunks are temporary
// - Buffers incomplete rows until more data arrives
// - Returns rows in batches to reduce NIF call overhead

use crate::core::{extract_field_owned_with_escape, is_separator};

/// State for streaming CSV parser
pub struct StreamingParser {
    /// Buffer holding unprocessed data
    buffer: Vec<u8>,
    /// Complete rows ready to be taken
    complete_rows: Vec<Vec<Vec<u8>>>,
    /// Position where the current (incomplete) row starts
    partial_row_start: usize,
    /// Position where we left off scanning (resume point)
    scan_pos: usize,
    /// Track if we're inside quotes (important for multi-chunk quoted fields)
    in_quotes: bool,
    /// Field separator characters (supports multiple separators for NimbleCSV compatibility)
    separators: Vec<u8>,
    /// Quote/escape character
    escape: u8,
}

impl StreamingParser {
    /// Create a new streaming parser with default settings (comma separator, double-quote escape)
    pub fn new() -> Self {
        Self::with_config(b',', b'"')
    }

    /// Create a new streaming parser with configurable separator and escape
    pub fn with_config(separator: u8, escape: u8) -> Self {
        StreamingParser {
            buffer: Vec::new(),
            complete_rows: Vec::new(),
            partial_row_start: 0,
            scan_pos: 0,
            in_quotes: false,
            separators: vec![separator],
            escape,
        }
    }

    /// Create a new streaming parser with multiple separator support
    pub fn with_multi_sep(separators: &[u8], escape: u8) -> Self {
        StreamingParser {
            buffer: Vec::new(),
            complete_rows: Vec::new(),
            partial_row_start: 0,
            scan_pos: 0,
            in_quotes: false,
            separators: separators.to_vec(),
            escape,
        }
    }

    /// Feed a chunk of data to the parser
    pub fn feed(&mut self, chunk: &[u8]) {
        // Append chunk to buffer
        self.buffer.extend_from_slice(chunk);

        // Process buffer to find complete rows
        self.process_buffer();
    }

    /// Process the buffer to extract complete rows
    fn process_buffer(&mut self) {
        // Resume from where we left off scanning
        let mut pos = self.scan_pos;
        let escape = self.escape;

        while pos < self.buffer.len() {
            let byte = self.buffer[pos];

            if self.in_quotes {
                if byte == escape {
                    if pos + 1 < self.buffer.len() && self.buffer[pos + 1] == escape {
                        pos += 2;
                        continue;
                    }
                    self.in_quotes = false;
                }
                pos += 1;
            } else if byte == escape {
                self.in_quotes = true;
                pos += 1;
            } else if byte == b'\n' {
                // Found end of row
                let row_end = pos;
                let row = self.parse_row_owned(self.partial_row_start, row_end);
                if !row.is_empty() {
                    self.complete_rows.push(row);
                }
                pos += 1;
                self.partial_row_start = pos;
                // Reset quote state for next row
                self.in_quotes = false;
            } else if byte == b'\r' {
                // Found end of row (CRLF or just CR)
                let row_end = pos;
                let row = self.parse_row_owned(self.partial_row_start, row_end);
                if !row.is_empty() {
                    self.complete_rows.push(row);
                }
                pos += 1;
                if pos < self.buffer.len() && self.buffer[pos] == b'\n' {
                    pos += 1;
                }
                self.partial_row_start = pos;
                // Reset quote state for next row
                self.in_quotes = false;
            } else {
                pos += 1;
            }
        }

        // Save scan position for resuming later
        self.scan_pos = pos;

        // Compact buffer: remove processed data to prevent unbounded growth
        if self.partial_row_start > 0 && self.partial_row_start >= self.buffer.len() / 2 {
            self.compact_buffer();
        }
    }

    /// Parse a row from buffer range into owned fields
    fn parse_row_owned(&self, start: usize, end: usize) -> Vec<Vec<u8>> {
        if start >= end {
            return Vec::new();
        }

        let line = &self.buffer[start..end];
        let mut fields = Vec::new();
        let mut pos = 0;
        let mut field_start = 0;
        let mut in_quotes = false;
        let separators = &self.separators;
        let escape = self.escape;

        while pos < line.len() {
            let byte = line[pos];

            if in_quotes {
                if byte == escape {
                    if pos + 1 < line.len() && line[pos + 1] == escape {
                        pos += 2;
                        continue;
                    }
                    in_quotes = false;
                }
                pos += 1;
            } else if byte == escape {
                in_quotes = true;
                pos += 1;
            } else if is_separator(byte, separators) {
                fields.push(extract_field_owned_with_escape(
                    line,
                    field_start,
                    pos,
                    escape,
                ));
                pos += 1;
                field_start = pos;
            } else {
                pos += 1;
            }
        }

        // Last field
        fields.push(extract_field_owned_with_escape(
            line,
            field_start,
            pos,
            escape,
        ));

        fields
    }

    /// Compact buffer by removing already-processed data
    fn compact_buffer(&mut self) {
        if self.partial_row_start > 0 {
            self.buffer.drain(0..self.partial_row_start);
            // Adjust positions after compaction
            self.scan_pos -= self.partial_row_start;
            self.partial_row_start = 0;
        }
    }

    /// Take up to `max` complete rows from the parser
    pub fn take_rows(&mut self, max: usize) -> Vec<Vec<Vec<u8>>> {
        let take_count = max.min(self.complete_rows.len());
        self.complete_rows.drain(0..take_count).collect()
    }

    /// Check how many complete rows are available
    pub fn available_rows(&self) -> usize {
        self.complete_rows.len()
    }

    /// Check if there's a partial row in the buffer
    pub fn has_partial(&self) -> bool {
        self.partial_row_start < self.buffer.len()
    }

    /// Get the size of buffered data (for memory monitoring)
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Finalize parsing - treat any remaining data as the last row
    pub fn finalize(&mut self) -> Vec<Vec<Vec<u8>>> {
        // Process any remaining partial row
        if self.partial_row_start < self.buffer.len() {
            let row = self.parse_row_owned(self.partial_row_start, self.buffer.len());
            if !row.is_empty() {
                self.complete_rows.push(row);
            }
            self.partial_row_start = self.buffer.len();
        }

        // Take all remaining rows
        std::mem::take(&mut self.complete_rows)
    }

    /// Reset the parser state
    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.complete_rows.clear();
        self.partial_row_start = 0;
        self.scan_pos = 0;
        self.in_quotes = false;
        // separator and escape are preserved
    }

    /// Get the separators
    #[allow(dead_code)]
    pub fn separators(&self) -> &[u8] {
        &self.separators
    }

    /// Get the first separator (for backward compatibility)
    #[allow(dead_code)]
    pub fn separator(&self) -> u8 {
        self.separators.first().copied().unwrap_or(b',')
    }

    /// Get the escape character
    #[allow(dead_code)]
    pub fn escape(&self) -> u8 {
        self.escape
    }
}

impl Default for StreamingParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_simple() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,b,c\n1,2,3\n");

        let rows = parser.take_rows(10);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(rows[1], vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn test_streaming_chunked() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,b,");
        assert_eq!(parser.available_rows(), 0);

        parser.feed(b"c\n1,2,3\n");
        assert_eq!(parser.available_rows(), 2);

        let rows = parser.take_rows(10);
        assert_eq!(rows[0], vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_streaming_quoted_across_chunks() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,\"hello ");
        assert_eq!(parser.available_rows(), 0);

        parser.feed(b"world\",c\n");
        assert_eq!(parser.available_rows(), 1);

        let rows = parser.take_rows(10);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"hello world".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_streaming_finalize() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,b,c\n1,2,3");

        // Take complete row first
        let rows1 = parser.take_rows(10);
        assert_eq!(rows1.len(), 1);

        // Finalize to get partial row
        let rows2 = parser.finalize();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0], vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn test_streaming_crlf() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,b\r\nc,d\r\n");

        let rows = parser.take_rows(10);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_take_rows_partial() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a\nb\nc\nd\n");

        let rows1 = parser.take_rows(2);
        assert_eq!(rows1.len(), 2);

        let rows2 = parser.take_rows(10);
        assert_eq!(rows2.len(), 2);
    }

    #[test]
    fn test_streaming_escaped_quotes() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,\"say \"\"hi\"\"\",c\n");

        let rows = parser.take_rows(10);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"say \"hi\"".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_streaming_multiline_quoted() {
        let mut parser = StreamingParser::new();
        parser.feed(b"a,\"line1\nline2\",c\n");

        let rows = parser.take_rows(10);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![b"a".to_vec(), b"line1\nline2".to_vec(), b"c".to_vec()]
        );
    }
}

// ResourceArc wrapper for streaming parser
//
// This allows the streaming parser state to persist across NIF calls.

use crate::strategy::StreamingParser;
use rustler::ResourceArc;
use std::sync::Mutex;

/// Wrapper for StreamingParser that can be stored in a ResourceArc
pub struct StreamingParserResource {
    pub inner: Mutex<StreamingParser>,
}

impl StreamingParserResource {
    pub fn new() -> Self {
        StreamingParserResource {
            inner: Mutex::new(StreamingParser::new()),
        }
    }

    pub fn with_config(separator: u8, escape: u8) -> Self {
        StreamingParserResource {
            inner: Mutex::new(StreamingParser::with_config(separator, escape)),
        }
    }

    pub fn with_multi_sep(separators: &[u8], escape: u8) -> Self {
        StreamingParserResource {
            inner: Mutex::new(StreamingParser::with_multi_sep(separators, escape)),
        }
    }
}

impl Default for StreamingParserResource {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for the ResourceArc
pub type StreamingParserRef = ResourceArc<StreamingParserResource>;

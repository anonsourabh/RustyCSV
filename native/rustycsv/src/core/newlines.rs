/// Custom newline support for CSV parsing.
///
/// When `is_default` is true, the standard \r\n and \n handling is used
/// (SIMD-optimized paths). When false, the general byte-by-byte parser
/// checks against the custom patterns.

#[derive(Debug, Clone)]
pub struct Newlines {
    /// Newline patterns sorted longest-first for greedy matching.
    pub patterns: Vec<Vec<u8>>,
    /// True when patterns are the default ["\r\n", "\n"].
    pub is_default: bool,
}

impl Newlines {
    /// Default newlines: \r\n and \n (handled by existing optimized code paths).
    pub fn default_newlines() -> Self {
        Newlines {
            patterns: vec![b"\r\n".to_vec(), b"\n".to_vec()],
            is_default: true,
        }
    }

    /// Custom newline patterns. Sorts longest-first for greedy matching.
    pub fn custom(mut patterns: Vec<Vec<u8>>) -> Self {
        // Sort longest-first so greedy matching works correctly
        patterns.sort_by_key(|b| std::cmp::Reverse(b.len()));
        Newlines {
            patterns,
            is_default: false,
        }
    }

    /// Maximum pattern length (used for chunk-boundary safety in streaming).
    pub fn max_pattern_len(&self) -> usize {
        self.patterns.iter().map(|p| p.len()).max().unwrap_or(1)
    }
}

/// Returns length of matched newline at `pos`, or 0 if no match.
#[inline]
pub fn match_newline(input: &[u8], pos: usize, newlines: &Newlines) -> usize {
    for pattern in &newlines.patterns {
        if input[pos..].starts_with(pattern) {
            return pattern.len();
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_newlines() {
        let nl = Newlines::default_newlines();
        assert!(nl.is_default);
        assert_eq!(nl.patterns.len(), 2);
    }

    #[test]
    fn test_custom_newlines() {
        let nl = Newlines::custom(vec![b"|".to_vec(), b"<br>".to_vec()]);
        assert!(!nl.is_default);
        // Should be sorted longest-first
        assert_eq!(nl.patterns[0], b"<br>".to_vec());
        assert_eq!(nl.patterns[1], b"|".to_vec());
    }

    #[test]
    fn test_match_newline_pipe() {
        let nl = Newlines::custom(vec![b"|".to_vec()]);
        let input = b"hello|world";
        assert_eq!(match_newline(input, 5, &nl), 1);
        assert_eq!(match_newline(input, 0, &nl), 0);
    }

    #[test]
    fn test_match_newline_multi_byte() {
        let nl = Newlines::custom(vec![b"<br>".to_vec()]);
        let input = b"hello<br>world";
        assert_eq!(match_newline(input, 5, &nl), 4);
        assert_eq!(match_newline(input, 0, &nl), 0);
    }

    #[test]
    fn test_match_newline_greedy() {
        // With patterns "|" and "||", "||" should match first at a "||" position
        let nl = Newlines::custom(vec![b"|".to_vec(), b"||".to_vec()]);
        let input = b"a||b";
        assert_eq!(match_newline(input, 1, &nl), 2); // "||" matches (longest first)
    }

    #[test]
    fn test_max_pattern_len() {
        let nl = Newlines::custom(vec![b"|".to_vec(), b"<br>".to_vec()]);
        assert_eq!(nl.max_pattern_len(), 4);
    }
}

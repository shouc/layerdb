//! VectorDB core library.

/// Library version string exposed to the CLI.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_is_non_empty() {
        assert!(!VERSION.is_empty());
    }
}

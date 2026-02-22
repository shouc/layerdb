//! VectDB core library.

pub mod cluster;
pub mod columnar;
pub mod dataset;
pub mod ground_truth;
pub mod index;
pub mod linalg;
pub mod topology;
pub mod types;

pub use types::{Neighbor, VectorIndex, VectorRecord};

/// Library version string exposed to the CLI.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

//! # Duplicate File Detection System
//!
//! A comprehensive duplicate file detection and management tool built in Rust.
//! Uses a data-oriented design philosophy with Polars DataFrames and smol async runtime.

pub mod data;
pub mod systems;
pub mod memory;
pub mod cache;
pub mod query;
pub mod similarity;
pub mod error;
pub mod detector;

// Re-export main API types
pub use detector::{DuplicateDetector, DetectorConfig};
pub use error::{DetectorError, DetectorResult};
pub use data::{ScanState, RelationStore};
pub use query::Query;
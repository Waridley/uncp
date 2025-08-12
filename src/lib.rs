//! # Duplicate File Detection System
//!
//! A comprehensive duplicate file detection and management tool built in Rust.
//! Uses a data-oriented design philosophy with Polars DataFrames and smol async runtime.

pub mod cache;
pub mod data;
pub mod detector;
pub mod error;
pub mod memory;
pub mod paths;
pub mod query;
pub mod similarity;

pub mod ui;

pub mod engine;

pub mod systems;

// Re-export main API types
pub use data::{RelationStore, ScanState};
pub use detector::{DetectorConfig, DuplicateDetector};
pub use error::{DetectorError, DetectorResult};
pub use query::Query;

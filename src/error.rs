//! Error types for the duplicate detection system

use std::path::PathBuf;
use thiserror::Error;

/// Main error type for the duplicate detection system
#[derive(Debug, Error)]
pub enum DetectorError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    
    #[error("Memory limit exceeded: requested {requested} bytes, limit is {limit} bytes")]
    MemoryExhausted { requested: usize, limit: usize },
    
    #[error("Invalid file type for operation: {file_type} not supported for {operation}")]
    InvalidFileType { file_type: String, operation: String },
    
    #[error("Cache error: {0}")]
    Cache(#[from] CacheError),
    
    #[error("System error: {0}")]
    System(#[from] SystemError),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
}

/// Cache-specific errors
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Cache file corrupted: {path}")]
    Corrupted { path: PathBuf },
    
    #[error("Cache version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: u32, found: u32 },
    
    #[error("Cache invalidation failed: {reason}")]
    InvalidationFailed { reason: String },
    
    #[error("Cache write failed: {0}")]
    WriteFailed(#[from] std::io::Error),
}

/// System processing errors
#[derive(Debug, Error)]
pub enum SystemError {
    #[error("System dependency not met: {system} requires {dependency}")]
    DependencyNotMet { system: String, dependency: String },
    
    #[error("System execution failed: {system} - {reason}")]
    ExecutionFailed { system: String, reason: String },
    
    #[error("System timeout: {system} exceeded {timeout_secs} seconds")]
    Timeout { system: String, timeout_secs: u64 },
}

/// Convenience type alias for Results
pub type DetectorResult<T> = Result<T, DetectorError>;
pub type CacheResult<T> = Result<T, CacheError>;
pub type SystemResult<T> = Result<T, SystemError>;

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
	InvalidFileType {
		file_type: String,
		operation: String,
	},

	#[error("Cache error: {0}")]
	Cache(#[from] CacheError),

	#[error("System error: {0}")]
	System(#[from] SystemError),

	#[error("Configuration error: {0}")]
	Config(String),

	#[error("Invalid glob pattern '{pattern}': {reason}")]
	InvalidGlobPattern { pattern: String, reason: String },

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

	#[error("Cache I/O error: {0}")]
	Io(#[from] std::io::Error),

	#[error("Cache Polars error: {0}")]
	Polars(#[from] polars::error::PolarsError),

	#[error("Cache JSON error: {0}")]
	Json(#[from] serde_json::Error),
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

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn test_detector_error_display() {
		let error = DetectorError::Config("test config error".to_string());
		assert_eq!(error.to_string(), "Configuration error: test config error");

		let error = DetectorError::MemoryExhausted {
			requested: 1000,
			limit: 500,
		};
		assert_eq!(
			error.to_string(),
			"Memory limit exceeded: requested 1000 bytes, limit is 500 bytes"
		);

		let error = DetectorError::InvalidFileType {
			file_type: "binary".to_string(),
			operation: "text analysis".to_string(),
		};
		assert_eq!(
			error.to_string(),
			"Invalid file type for operation: binary not supported for text analysis"
		);
	}

	#[test]
	fn test_cache_error_display() {
		let error = CacheError::Corrupted {
			path: PathBuf::from("/test/cache.dat"),
		};
		assert_eq!(error.to_string(), "Cache file corrupted: /test/cache.dat");

		let error = CacheError::VersionMismatch {
			expected: 2,
			found: 1,
		};
		assert_eq!(
			error.to_string(),
			"Cache version mismatch: expected 2, found 1"
		);

		let error = CacheError::InvalidationFailed {
			reason: "file not found".to_string(),
		};
		assert_eq!(
			error.to_string(),
			"Cache invalidation failed: file not found"
		);
	}

	#[test]
	fn test_system_error_display() {
		let error = SystemError::DependencyNotMet {
			system: "hashing".to_string(),
			dependency: "discovery".to_string(),
		};
		assert_eq!(
			error.to_string(),
			"System dependency not met: hashing requires discovery"
		);

		let error = SystemError::ExecutionFailed {
			system: "discovery".to_string(),
			reason: "permission denied".to_string(),
		};
		assert_eq!(
			error.to_string(),
			"System execution failed: discovery - permission denied"
		);

		let error = SystemError::Timeout {
			system: "hashing".to_string(),
			timeout_secs: 30,
		};
		assert_eq!(
			error.to_string(),
			"System timeout: hashing exceeded 30 seconds"
		);
	}

	#[test]
	fn test_error_conversion() {
		// Test that std::io::Error converts to DetectorError
		let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
		let detector_error: DetectorError = io_error.into();
		assert!(matches!(detector_error, DetectorError::Io(_)));

		// Test that CacheError converts to DetectorError
		let cache_error = CacheError::InvalidationFailed {
			reason: "test".to_string(),
		};
		let detector_error: DetectorError = cache_error.into();
		assert!(matches!(detector_error, DetectorError::Cache(_)));

		// Test that SystemError converts to DetectorError
		let system_error = SystemError::ExecutionFailed {
			system: "test".to_string(),
			reason: "test".to_string(),
		};
		let detector_error: DetectorError = system_error.into();
		assert!(matches!(detector_error, DetectorError::System(_)));
	}
}

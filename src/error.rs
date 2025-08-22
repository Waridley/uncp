//! Error types for the duplicate detection system

use std::path::PathBuf;
use thiserror::Error;

/// Comprehensive error type covering all failure modes in the duplicate detection system.
///
/// `DetectorError` provides structured error handling with detailed context for
/// debugging and user-friendly error messages. Each variant includes specific
/// information about the failure condition and potential recovery strategies.
///
/// ## Error Categories
///
/// ### I/O Errors
/// File system operations, permission issues, disk space problems:
/// - File access denied or permission errors
/// - Disk full or insufficient space
/// - Network drive connectivity issues
/// - Corrupted file system metadata
///
/// ### Memory Errors
/// Memory allocation and management failures:
/// - System memory exhaustion
/// - Configured memory limits exceeded
/// - Large file processing failures
/// - Cache eviction pressure
///
/// ### Data Processing Errors
/// Issues with data analysis and computation:
/// - Polars DataFrame operation failures
/// - Hash computation errors
/// - Data serialization/deserialization issues
/// - Invalid file format detection
///
/// ### Configuration Errors
/// Invalid settings or configuration problems:
/// - Invalid glob patterns in path filters
/// - Unsupported file type operations
/// - Missing required dependencies
/// - Platform-specific configuration issues
///
/// ### Cache Errors
/// Persistent storage and cache management issues:
/// - Cache file corruption or version mismatches
/// - Atomic operation failures
/// - Cache invalidation problems
/// - Storage backend errors
///
/// ## Error Handling Patterns
///
/// ```rust
/// use uncp::{DetectorError, DuplicateDetector, DetectorConfig};
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut detector = DuplicateDetector::new(DetectorConfig::default())?;
///
/// match detector.scan_directory(PathBuf::from("./data")).await {
///     Ok(_) => println!("Scan completed successfully"),
///     Err(DetectorError::Io(io_err)) => {
///         eprintln!("I/O error during scan: {}", io_err);
///         // Handle permission issues, retry with different path, etc.
///     }
///     Err(DetectorError::MemoryExhausted { requested, limit }) => {
///         eprintln!("Memory limit exceeded: {} bytes requested, {} limit", requested, limit);
///         // Reduce batch size, increase memory limit, or process in chunks
///     }
///     Err(DetectorError::InvalidGlobPattern { pattern, reason }) => {
///         eprintln!("Invalid filter pattern '{}': {}", pattern, reason);
///         // Fix pattern syntax or use simpler patterns
///     }
///     Err(err) => {
///         eprintln!("Unexpected error: {}", err);
///         // Log for debugging, show user-friendly message
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// ## Recovery Strategies
///
/// Many errors include context for automatic or user-guided recovery:
/// - **Memory errors**: Suggest reducing batch sizes or increasing limits
/// - **I/O errors**: Provide retry mechanisms with exponential backoff
/// - **Permission errors**: Guide users to fix access rights
/// - **Configuration errors**: Suggest valid alternatives or defaults
/// - **Cache errors**: Automatic cache rebuilding or manual cleanup
#[derive(Debug, Error)]
pub enum DetectorError {
	/// File system I/O errors including permission issues and disk space problems
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	/// Polars DataFrame operation errors during data processing
	#[error("Polars error: {0}")]
	Polars(#[from] polars::error::PolarsError),

	/// Memory allocation failures when system or configured limits are exceeded
	#[error("Memory limit exceeded: requested {requested} bytes, limit is {limit} bytes")]
	MemoryExhausted { requested: usize, limit: usize },

	/// Operations attempted on unsupported file types
	#[error("Invalid file type for operation: {file_type} not supported for {operation}")]
	InvalidFileType {
		file_type: String,
		operation: String,
	},

	/// Cache-related errors including corruption and version mismatches
	#[error("Cache error: {0}")]
	Cache(#[from] CacheError),

	/// System-level errors including platform-specific issues
	#[error("System error: {0}")]
	System(#[from] SystemError),

	/// Configuration validation errors with descriptive messages
	#[error("Configuration error: {0}")]
	Config(String),

	/// Invalid glob pattern syntax in path filters
	#[error("Invalid glob pattern '{pattern}': {reason}")]
	InvalidGlobPattern { pattern: String, reason: String },

	/// Data serialization/deserialization errors
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

/// Convenience type alias for Results in the duplicate detection system.
///
/// `DetectorResult<T>` is equivalent to `Result<T, DetectorError>` and is used
/// throughout the API to provide consistent error handling. This type alias
/// simplifies function signatures and makes error handling patterns more readable.
///
/// ## Usage
///
/// ```rust
/// use uncp::{DetectorResult, DuplicateDetector, DetectorConfig};
///
/// fn create_detector() -> DetectorResult<DuplicateDetector> {
///     DuplicateDetector::new(DetectorConfig::default())
/// }
///
/// # async fn example() -> DetectorResult<()> {
/// let detector = create_detector()?;
/// // ... use detector
/// Ok(())
/// # }
/// ```
pub type DetectorResult<T> = Result<T, DetectorError>;

/// Convenience type alias for cache operation results.
pub type CacheResult<T> = Result<T, CacheError>;

/// Convenience type alias for system operation results.
pub type SystemResult<T> = Result<T, SystemError>;

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test_log::test]
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

	#[test_log::test]
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

	#[test_log::test]
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

	#[test_log::test]
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

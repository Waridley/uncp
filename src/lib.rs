//! # uncp - Un-copy (Duplicate File Detection)
// NOTE: this module list is used to control documentation order

//!
//! A tool to help you de-duplicate your data. Multiple similarity metrics, asynchronous scanning,
//! and high-performance file analysis. Builds a graph of file relationships for flexible querying.
//! Data-oriented design backed by Polars DataFrames. Multiple ways to use, including as a Rust
//! library, CLI, GUI, and TUI.
//!
//! ## Architecture
//!
//! The library is organized into several key modules:
//!
//! - **Core Data Layer**: [`data`] - Polars DataFrame-based data structures for file metadata and relationships
//! - **Detection Engine**: [`detector`] - Main API for duplicate detection with configurable processing
//! - **Processing Systems**: [`systems`] - Modular pipeline for file discovery, hashing, and similarity analysis
//! - **Caching Layer**: [`persist`] - Persistent storage using Parquet format with atomic operations
//! - **Memory Management**: [`memory`] - LRU caching with configurable limits and system-aware defaults
//! - **Background Engine**: [`engine`] - Async processing engine with progress callbacks and command interface
//! - **Query Interface**: [`query`] - High-level API for accessing processed data and relationships
//! - **User Interface**: [`ui`] - Shared presentation layer for GUI/TUI clients
//! - **Error Handling**: [`error`] - Comprehensive error types with context and recovery information
//! - **Utilities**: [`paths`], [`similarity`] - Common helpers and extensibility interfaces
//!
//! ## Quick Start
//!
//! ### Basic Usage
//!
//! ```rust
//! use uncp::{DuplicateDetector, DetectorConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create detector with default configuration
//! let mut detector = DuplicateDetector::new(DetectorConfig::default())?;
//!
//! // Scan a directory for files
//! detector.scan_directory("./test_data".into()).await?;
//!
//! // Process until complete (includes hashing)
//! detector.process_until_complete().await?;
//!
//! // Query results
//! let query = detector.query();
//! let text_files = query.files_by_type("text")?;
//! let files_needing_hash = query.files_needing_hashing()?;
//!
//! println!("Found {} text files", text_files.height());
//! println!("{} files need hashing", files_needing_hash.height());
//! # Ok(())
//! # }
//! ```
//!
//! ### Advanced Configuration
//!
//! ```rust
//! use uncp::{DuplicateDetector, DetectorConfig, PathFilter};
//!
//! # async fn advanced_example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure memory limits and processing options
//! let config = DetectorConfig::default();
//!
//! let mut detector = DuplicateDetector::new(config)?;
//!
//! // Set up path filters to exclude certain files
//! let filter = PathFilter::new(vec!["**/*.{jpg,png,mp4}".to_string()], vec![
//!     "**/node_modules/**".to_string(),
//!     "**/.git/**".to_string(),
//! ])?;
//!
//! detector.config.path_filter = filter;
//!
//! // Scan with progress reporting
//! detector
//!     .scan_with_progress("./media".into(), std::sync::Arc::new(|progress| {
//!         println!(
//!             "{}: {}/{}",
//!             progress.system_name, progress.processed_items, progress.total_items
//!         );
//!     }))
//!     .await?;
//!
//! // Continue processing until complete
//! detector
//!     .process_until_complete_with_progress(
//!         None,
//!         std::sync::Arc::new(|progress| {
//!             println!(
//!                 "{}: {}/{}",
//!                 progress.system_name, progress.processed_items, progress.total_items
//!             );
//!         }),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Background Processing with Engine
//!
//! ```rust
//! use uncp::{engine::{BackgroundEngine, EngineCommand, EngineMode}, DetectorConfig, DuplicateDetector};
//! use std::path::PathBuf;
//!
//! # async fn engine_example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create background engine for async processing
//! let detector = DuplicateDetector::new(DetectorConfig::default())?;
//! let (_engine, mut events, command_tx) =
//!     BackgroundEngine::start_with_mode(detector, EngineMode::Interactive);
//!
//! // Send commands to control processing
//! command_tx.send(EngineCommand::SetPath(PathBuf::from("./data"))).await?;
//! command_tx.send(EngineCommand::Start).await?;
//!
//! // Stop the engine
//! command_tx.send(EngineCommand::Stop).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! ### Performance & Scalability
//! - **High Performance**: Multi-threaded processing with configurable concurrency
//! - **Memory Efficient**: LRU caching with automatic memory management and system-aware limits
//! - **Lazy Evaluation**: Polars DataFrames enable processing of datasets larger than RAM
//! - **Parallel I/O**: Concurrent file discovery and hashing with work-stealing scheduler
//! - **Blake3 Hashing**: Fast, cryptographically secure content hashing
//! - **Incremental Processing**: Smart caching avoids re-processing unchanged files
//!
//! ### Cross-Platform Compatibility
//! - **Windows**: NTFS support, long path handling, atomic operations via temp files
//! - **macOS**: HFS+/APFS optimization, native atomic rename operations
//! - **Linux**: ext4/btrfs/xfs support, POSIX compliance, efficient memory mapping
//! - **Unicode**: Full Unicode path support with normalization across platforms
//! - **Permissions**: Graceful handling of access restrictions and permission errors
//!
//! ### Data Integrity & Reliability
//! - **Atomic Operations**: Safe concurrent access with atomic cache operations
//! - **Crash Recovery**: Automatic detection and recovery from incomplete operations
//! - **Data Validation**: Comprehensive integrity checking for cached data
//! - **Error Recovery**: Graceful degradation and detailed error reporting
//! - **Concurrent Safety**: Thread-safe operations with minimal lock contention
//!
//! ### User Experience
//! - **Real-Time Monitoring**: Progress callbacks and detailed status reporting
//! - **Multiple Interfaces**: CLI, GUI (Iced), and TUI (Ratatui) frontends
//! - **Interactive Control**: Start, stop, pause operations with immediate response
//! - **Flexible Filtering**: Glob patterns for including/excluding files and directories
//! - **Comprehensive Logging**: Structured logging with configurable verbosity
//!
//! ### Extensibility & Integration
//! - **Plugin Architecture**: Custom similarity providers for domain-specific analysis
//! - **API Design**: Clean, composable API suitable for integration into larger systems
//! - **Data Export**: Parquet format enables integration with data analysis tools
//! - **Async Support**: Full async/await support with smol runtime integration
//! - **Memory Profiling**: Built-in memory usage tracking and optimization hints
//!
//! ## Data Processing Pipeline
//!
//! The duplicate detection system uses a multi-stage pipeline optimized for performance:
//!
//! 1. **Discovery**: [`systems::FileDiscoverySystem`] scans filesystem with configurable filters
//!    - Parallel directory traversal with work-stealing scheduler
//!    - Glob-based filtering to exclude unwanted files early
//!    - Metadata collection (size, modification time, permissions)
//!    - Typical performance: 10,000+ files/second on modern SSDs
//!
//! 2. **Hashing**: [`systems::ContentHashSystem`] computes Blake3 hashes for content comparison
//!    - Parallel hashing with configurable thread pool
//!    - Memory-mapped I/O for large files, buffered reads for small files
//!    - Progress reporting with file-level granularity
//!    - Typical performance: 500MB/second per thread on modern hardware
//!
//! 3. **Similarity**: Extensible providers for perceptual hashing, text diffing, etc.
//!    - Plugin architecture for domain-specific analysis
//!    - Configurable similarity thresholds and algorithms
//!    - Batch processing for efficiency
//!
//! 4. **Caching**: Persistent storage with intelligent invalidation and merging
//!    - Parquet format for efficient columnar storage and compression
//!    - Atomic write operations prevent corruption from crashes
//!    - Automatic cache validation and cleanup of stale entries
//!    - Incremental updates avoid full re-processing
//!
//! 5. **Querying**: High-level API for accessing results and relationships
//!    - Lazy evaluation enables processing datasets larger than RAM
//!    - Optimized queries for common operations (duplicates, file types, sizes)
//!    - DataFrame-based results for further analysis and export
//!
//! ## Performance Characteristics
//!
//! Typical performance on modern hardware (SSD storage, 16GB RAM, 8-core CPU):
//!
//! - **File Discovery**: 10,000-50,000 files/second depending on filesystem
//! - **Content Hashing**: 500MB/second per thread (scales with CPU cores)
//! - **Memory Usage**: 50-100MB base + ~100 bytes per file in dataset
//! - **Cache Loading**: 100,000+ files/second from Parquet cache
//! - **Query Performance**: Sub-second response for millions of files
//!
//! The system is designed to scale from small personal collections (thousands of files)
//! to enterprise datasets (millions of files) while maintaining responsive performance.
//!
//! ## Memory Management
//!
//! The system uses multiple strategies to manage memory efficiently:
//!
//! - **Lazy DataFrames**: Process datasets larger than available RAM
//! - **LRU Caching**: Automatic eviction of least-recently-used data
//! - **System Monitoring**: Adapts memory usage based on available system resources
//! - **Configurable Limits**: User-defined memory limits with automatic enforcement
//! - **Streaming Processing**: Large files processed in chunks to avoid memory spikes

// test-log initializes tracing automatically for unit tests.
// No manual tracing init needed here.

// Core modules - fundamental data structures and processing

/// Core data structures using Polars DataFrames for efficient file metadata storage.
///
/// This module provides the foundational data types for the duplicate detection system:
///
/// - [`ScanState`]: Primary DataFrame containing file metadata (paths, sizes, types, hashes)
/// - [`RelationStore`]: Manages similarity groups and pairwise file relationships
/// - [`FileType`]: Enumeration of detected file types (text, binary, unknown)
///
/// Both main structures support:
/// - Efficient columnar operations via Polars
/// - Serialization to Parquet format for caching
/// - Deduplication and merging of data from multiple scans
/// - Memory-efficient lazy evaluation for large datasets
///
/// The data layer is designed for high-performance analytics workloads with
/// millions of files while maintaining low memory overhead through lazy evaluation.
pub mod data;

/// Main duplicate detection API with configurable processing pipeline.
///
/// This module contains the primary user-facing API for duplicate file detection:
///
/// - [`DuplicateDetector`]: Main API for scanning, hashing, and analyzing files
/// - [`DetectorConfig`]: Configuration for memory limits, concurrency, and processing
/// - [`PathFilter`]: Glob-based filtering for including/excluding files during scans
///
/// The detector orchestrates the entire processing pipeline:
/// 1. File discovery with configurable filters
/// 2. Content hashing using Blake3 for fast, secure comparison
/// 3. Similarity analysis with extensible provider system
/// 4. Intelligent caching with atomic operations
/// 5. Query interface for accessing results
///
/// Supports both synchronous and asynchronous operation modes with progress reporting.
pub mod detector;

/// Comprehensive error handling with detailed context and recovery information.
///
/// This module provides structured error types for all failure modes in the system:
///
/// - [`DetectorError`]: Main error type with variants for different failure categories
/// - [`DetectorResult<T>`]: Convenience type alias for `Result<T, DetectorError>`
///
/// Error categories include:
/// - I/O errors (file access, permission issues, disk space)
/// - Memory errors (allocation failures, cache exhaustion)
/// - Configuration errors (invalid settings, missing dependencies)
/// - Processing errors (hash computation, data corruption)
/// - System errors (platform-specific failures)
///
/// All errors include detailed context for debugging and user-friendly messages
/// for display in CLI/GUI interfaces.
pub mod error;

/// Type-safe relationship management system for file relationships.
///
/// This module provides a flexible system for storing and retrieving different types
/// of file relationships using compile-time type safety. Each relation type is
/// identified by a unique type that implements the `RelationKey` trait.
///
/// Key features:
/// - **Type Safety**: Relations are retrieved using compile-time type checking
/// - **Extensibility**: New relation types can be added without modifying core code
/// - **Metadata**: Each relation includes descriptive metadata and versioning
/// - **Performance**: Efficient storage and retrieval using TypeId-based indexing
///
/// Example relation types:
/// - `IdenticalHashes`: Files with identical content hashes
/// - `SameFileName`: Files with identical names but different paths
/// - `SimilarContent`: Files with similar content based on analysis
/// - `SameSize`: Files with identical sizes
///
/// The system supports arbitrary relation types defined by users or plugins,
/// making it highly extensible for domain-specific duplicate detection needs.
pub mod relations;

// Processing pipeline - modular systems for file analysis

/// Modular processing systems for the file analysis pipeline.
///
/// This module implements a data-oriented processing architecture with specialized systems:
///
/// - [`FileDiscoverySystem`]: Parallel filesystem traversal with configurable filters
/// - [`ContentHashSystem`]: Blake3 hashing with progress reporting and memory management
/// - [`SystemScheduler`]: Orchestrates system execution with dependency management
///
/// Key features:
/// - **Parallel Processing**: Multi-threaded execution with configurable concurrency
/// - **Progress Reporting**: Real-time callbacks for UI integration
/// - **Memory Management**: Automatic batching and memory pressure handling
/// - **Error Recovery**: Graceful handling of I/O errors and permission issues
/// - **Extensibility**: Plugin architecture for custom processing systems
///
/// The systems operate on shared [`ScanState`] and [`RelationStore`] data structures,
/// enabling efficient data flow and minimal memory allocation during processing.
///
/// Example processing flow:
/// 1. FileDiscoverySystem scans filesystem and populates file metadata
/// 2. ContentHashSystem computes hashes for content-based comparison
/// 3. Custom similarity systems can add perceptual hashing, text analysis, etc.
/// 4. Results are automatically cached for subsequent runs
pub mod systems;

// Infrastructure - caching, memory management, and utilities

/// Persistent caching system using Parquet format with atomic operations.
///
/// This module provides enterprise-grade caching for file metadata and processing results:
///
/// - [`CacheManager`]: Main interface for cache operations with atomic guarantees
/// - [`CacheConfig`]: Configuration for cache location, retention, and validation
/// - Cross-platform atomic writes using write-to-temp-then-rename pattern
///
/// Key features:
/// - **Atomic Operations**: Guaranteed consistency even with concurrent access
/// - **Intelligent Validation**: Automatic detection of stale or corrupted cache data
/// - **Efficient Merging**: Combines data from multiple cache files without duplication
/// - **Cross-Platform**: Windows, macOS, and Linux support with platform optimizations
/// - **Parquet Format**: Columnar storage for fast queries and compression
///
/// The cache system automatically handles:
/// - File modification detection and cache invalidation
/// - Cleanup of deleted files from cached data
/// - Merging of incremental scan results
/// - Recovery from partial writes and corruption
///
/// Cache files are stored in a user-configurable directory with automatic cleanup
/// of old entries based on configurable retention policies.
pub mod persist;

/// LRU caching with configurable limits and system-aware memory management.
///
/// This module provides in-memory caching for frequently accessed data:
///
/// - [`MemoryManager`]: System memory monitoring and automatic limit adjustment
/// - [`LruCache<K, V>`]: Generic LRU cache with configurable capacity and eviction
/// - System-aware defaults based on available RAM and platform characteristics
///
/// Features:
/// - **Automatic Sizing**: Adapts cache limits based on system memory availability
/// - **Memory Pressure Handling**: Proactive eviction when system memory is low
/// - **Thread Safety**: Concurrent access with minimal lock contention
/// - **Metrics**: Cache hit rates, eviction counts, and memory usage tracking
///
/// The memory manager monitors system resources and automatically adjusts
/// cache sizes to prevent memory exhaustion while maximizing performance.
/// Different cache instances can be configured with different priorities
/// for intelligent resource allocation.
pub mod memory;

/// Path manipulation utilities and filesystem helpers.
///
/// This module provides cross-platform path handling and filesystem utilities:
///
/// - Path normalization and canonicalization
/// - Glob pattern matching for file filtering
/// - Cross-platform path separator handling
/// - Symlink detection and resolution
/// - Permission checking and access validation
///
/// Utilities handle platform-specific filesystem quirks and provide
/// consistent behavior across Windows, macOS, and Linux systems.
/// Special handling for case-sensitive vs case-insensitive filesystems,
/// Unicode normalization, and long path support on Windows.
pub mod paths;

// Query and presentation - data access and user interfaces

/// High-level query interface for accessing processed file data and relationships.
///
/// This module provides a convenient API for querying the underlying DataFrames:
///
/// - [`Query`]: Main query interface with methods for filtering and analysis
/// - Immutable access to [`ScanState`] and [`RelationStore`] data
/// - Efficient lazy evaluation for complex queries
///
/// Query capabilities:
/// - **File Filtering**: By type, size, path patterns, hash status
/// - **Duplicate Analysis**: Groups of identical files, similarity relationships
/// - **Statistics**: File counts, size distributions, type breakdowns
/// - **Path-Based Queries**: Files under specific directories or matching patterns
///
/// All queries return Polars DataFrames for efficient further processing.
/// The query interface is designed for both programmatic use and interactive
/// exploration in CLI/GUI applications.
///
/// Example queries:
/// - Files larger than 1MB that need hashing
/// - All duplicate groups with more than 2 files
/// - Text files under a specific directory
/// - Files with identical content hashes
pub mod query;

/// Shared presentation layer for GUI and TUI client interfaces.
///
/// This module provides data structures for presenting file information in user interfaces:
///
/// - [`PresentationState`]: Aggregated view of detector state for UI display
/// - File type breakdowns with counts and percentages
/// - Progress information for ongoing operations
/// - Filtered views for specific directories or file types
///
/// Features:
/// - **UI-Friendly Data**: Pre-computed aggregations and formatted strings
/// - **Path Filtering**: Scoped views for directory-specific analysis
/// - **Progress Tracking**: Real-time updates for long-running operations
/// - **Sorting and Grouping**: Optimized for common UI display patterns
///
/// The presentation layer abstracts the underlying DataFrame complexity
/// and provides stable, UI-friendly data structures that can be efficiently
/// updated and displayed in real-time interfaces.
///
/// Supports both full dataset views and filtered views for performance
/// when displaying large numbers of files in GUI/TUI applications.
pub mod ui;

// Background processing - async engine with progress reporting

/// Asynchronous background processing engine with progress callbacks and command interface.
///
/// This module provides the async processing engine for long-running operations:
///
/// - [`BackgroundEngine`]: Main async engine for file processing
/// - [`EngineCommand`]: Command interface for controlling engine operation
/// - [`EngineMode`]: Configuration for different operational modes
/// - [`ProgressCallback`]: Real-time progress reporting for UI integration
///
/// Engine capabilities:
/// - **Async Processing**: Non-blocking operation with smol async runtime
/// - **Progress Reporting**: Real-time callbacks for discovery, hashing, and analysis
/// - **Command Interface**: Start, stop, pause, and configuration commands
/// - **Error Recovery**: Graceful handling of I/O errors and system failures
/// - **Resource Management**: Automatic memory and CPU throttling
///
/// The engine operates independently of the main thread, communicating
/// through channels for commands and progress updates. This enables
/// responsive UI interfaces while processing large datasets.
///
/// Supports different operational modes:
/// - **CLI Mode**: Batch processing with minimal overhead
/// - **GUI Mode**: Real-time updates with detailed progress information
/// - **TUI Mode**: Interactive processing with user control
///
/// The engine automatically manages system resources and can be configured
/// for different performance profiles based on system capabilities.
pub mod engine;

/// Content store providing ref-counted loading/unloading of file bytes.
pub mod content;

/// Content load request queue (in-memory) with de-duplication
pub mod content_queue;

/// Shared thread-safe data pool for systems
pub mod pool;

/// Event bus for system-to-system communication.
/// Systems emit broadcast events that UIs and other systems can subscribe to.
pub mod events;

// Extensibility - plugin interfaces for custom functionality

/// Plugin interfaces for custom similarity providers and extensibility.
///
/// This module defines trait interfaces for extending the duplicate detection system:
///
/// - [`SimilarityProvider`]: Interface for custom similarity analysis
/// - Plugin architecture for perceptual hashing, text diffing, etc.
/// - Extensible processing pipeline for domain-specific analysis
///
/// The similarity system is designed to be extensible with custom providers
/// for different types of content analysis:
/// - **Perceptual Hashing**: Image similarity detection
/// - **Text Analysis**: Document similarity and plagiarism detection
/// - **Audio Fingerprinting**: Music and audio duplicate detection
/// - **Video Analysis**: Frame-based video similarity
///
/// Custom providers can be registered with the detector and will be
/// automatically integrated into the processing pipeline with progress
/// reporting and caching support.
///
/// The trait-based design allows for compile-time optimization while
/// maintaining flexibility for runtime plugin loading in future versions.
pub mod similarity;

// Re-export main API types for convenient access
pub use data::{RelationStore, ScanState};
pub use detector::{DetectorConfig, DuplicateDetector, PathFilter};
pub use engine::EngineMode;
pub use error::{DetectorError, DetectorResult};

pub mod log_ui;
pub use log_ui::{UiErrorEvent, UiErrorQueueHandle, install_ui_error_layer};

pub use events::{EventBus, SystemEvent};
pub use query::Query;
pub use relations::{
	IdenticalHashes, RelationKey, RelationMetadata, SameFileName, SameSize, SimilarityGroups,
};

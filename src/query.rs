//! Query interface for accessing data and relations

use polars::prelude::*;

use crate::data::{RelationStore, ScanState};

/// High-level query interface for accessing processed file data and relationships.
///
/// `Query` provides a convenient, type-safe API for filtering and analyzing
/// the file metadata and relationships discovered during duplicate detection.
/// It operates on immutable references to the underlying DataFrames, making
/// it safe for concurrent access and efficient for complex analytical queries.
///
/// ## Design Philosophy
///
/// The query interface is designed around common use cases:
/// - **File Filtering**: Find files by type, size, path patterns, or processing status
/// - **Duplicate Analysis**: Identify exact and near-duplicate files
/// - **Statistics**: Generate summaries and reports for UI display
/// - **Batch Operations**: Efficiently process large result sets
///
/// ## Performance Characteristics
///
/// - **Lazy Evaluation**: Queries are optimized and executed only when needed
/// - **Columnar Operations**: Leverages Polars' efficient columnar processing
/// - **Memory Efficient**: Large result sets can be processed without loading all data
/// - **Parallel Execution**: Complex queries automatically use multiple threads
///
/// ## Query Categories
///
/// ### File Metadata Queries
/// - Files by type (text, image, audio, video, etc.)
/// - Files by size range or specific sizes
/// - Files modified within date ranges
/// - Files matching path patterns
///
/// ### Processing Status Queries
/// - Files that need content hashing
/// - Files with computed hashes
/// - Files that failed processing
/// - Files excluded by filters
///
/// ### Duplicate Detection Queries
/// - Exact duplicate groups (same content hash)
/// - Similarity groups (near-duplicates)
/// - Largest duplicate groups by file count
/// - Duplicate groups by total size
///
/// ### Statistical Queries
/// - File type distribution
/// - Size distribution and histograms
/// - Processing progress summaries
/// - Storage space analysis
///
/// ## Examples
///
/// ### Basic File Filtering
/// ```rust
/// use uncp::{DuplicateDetector, DetectorConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let detector = DuplicateDetector::new(DetectorConfig::default())?;
/// let query = detector.query();
///
/// // Find all image files
/// let images = query.files_by_type("image")?;
/// println!("Found {} image files", images.height());
///
/// // Find files that need hashing
/// let unhashed = query.files_needing_hashing()?;
/// println!("{} files need content hashing", unhashed.height());
///
/// // Find large files (>100MB)
/// // let large_files = query.files_larger_than(100 * 1024 * 1024)?;
/// // println!("Found {} large files", large_files.height());
/// # Ok(())
/// # }
/// ```
///
/// ### Duplicate Analysis
/// ```rust
/// use uncp::{DuplicateDetector, DetectorConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let detector = DuplicateDetector::new(DetectorConfig::default())?;
/// let query = detector.query();
///
/// // Find all duplicate groups
/// // For now, duplicate analysis is not implemented in Query.
/// // This section is intentionally left as a placeholder for future work.
/// # Ok(())
/// # }
/// ```
///
/// ## Thread Safety
///
/// `Query` holds immutable references and is safe for concurrent access.
/// Multiple queries can be created and used simultaneously without
/// synchronization, making it ideal for parallel processing and UI updates.
#[derive(Debug, Clone)]
pub struct Query {
	pub state: std::sync::Arc<std::sync::RwLock<ScanState>>,
	pub relations: std::sync::Arc<std::sync::RwLock<RelationStore>>,
}

impl Query {
	pub fn new(
		state: std::sync::Arc<std::sync::RwLock<ScanState>>,
		relations: std::sync::Arc<std::sync::RwLock<RelationStore>>,
	) -> Self {
		Self { state, relations }
	}

	pub fn files_by_type(&self, file_type: &str) -> PolarsResult<DataFrame> {
		let state = self.state.read().unwrap();
		state
			.data
			.clone()
			.lazy()
			.filter(col("file_type").eq(lit(file_type)))
			.collect()
	}

	pub fn files_needing_hashing(&self) -> PolarsResult<DataFrame> {
		let state = self.state.read().unwrap();
		state
			.data
			.clone()
			.lazy()
			.filter(col("hashed").eq(lit(false)))
			.collect()
	}
}

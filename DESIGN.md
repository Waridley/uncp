# Duplicate File Detection System - Design Document

## 1. Executive Summary

This document outlines the architecture for a comprehensive duplicate file detection and management tool built in Rust. The system employs a data-oriented design philosophy inspired by Entity-Component-System (ECS) patterns, using Polars DataFrames as the primary data store and smol for asynchronous processing.

### Key Design Principles
- **Data-oriented design**: Data is pure data with no inherent functionality
- **System-based processing**: Independent systems operate on whatever data columns they need
- **Relational data modeling**: Relationships between files are first-class data structures
- **Async-first architecture**: Built around smol's async runtime for responsive processing
- **Memory-aware processing**: Configurable memory constraints with intelligent caching

## 2. System Architecture Overview

### 2.1 Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Client    │    │   GUI Client    │    │   TUI Client    │
│   (Pipelined)   │    │  (Responsive)   │    │  (Responsive)   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼───────────────┐
                    │     Core Library API       │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   DuplicateDetector        │
                    │   - ScanState              │
                    │   - SystemScheduler        │
                    │   - MemoryManager          │
                    │   - RelationStore          │
                    │   - CacheManager           │
                    └─────────────┬───────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
    ┌─────▼─────┐         ┌───────▼───────┐       ┌───────▼───────┐
    │   Data    │         │    Systems    │       │   Relations   │
    │   Store   │         │   Scheduler   │       │     Store     │
    └───────────┘         └───────────────┘       └───────────────┘
```

### 2.2 Data Flow Architecture

```
File Discovery → Content Analysis → Hash Computation → Similarity Analysis → Relation Building
      │                │                  │                   │                    │
      ▼                ▼                  ▼                   ▼                    ▼
   ScanState       ScanState         ScanState          ScanState           RelationStore
   (files)         (content)         (hashes)          (scores)            (groups)
```

## 3. Data Architecture

### 3.1 Unified Data Store

The system uses a single primary DataFrame in `ScanState` containing all file metadata, with separate relational DataFrames for relationships between files.

#### Primary Data Schema
```rust
// Core file identity and metadata
path: String                     // Unique file path
size: UInt64                     // File size in bytes
modified: Datetime               // Last modified timestamp
file_type: String                // FileKind enum as string

// Processing state flags (ECS-style components)
content_loaded: Boolean          // Has file content been loaded
hashed: Boolean                  // Have hashes been computed
similarity_computed: Boolean     // Has similarity analysis been done

// Scan metadata
scan_id: UInt32                  // Incremental scan identifier
last_processed: Datetime         // When this file was last processed
```

#### Hash Data (stored in main DataFrame)
```rust
blake3_hash: String             // Exact content hash
perceptual_hash: String         // Image perceptual hash
text_hash: String               // Text content hash
```

### 3.2 Relational Data Store

#### Hash Relations (Value-Indexed)
```rust
hash_value: String             // The actual hash
hash_type: String              // "blake3", "perceptual", etc.
file_paths: List<String>       // All files with this hash
first_seen: Datetime           // When first discovered
file_count: UInt32             // Number of files with this hash
```

#### Similarity Groups (Group-Indexed)
```rust
group_id: String               // UUID or hash of group
group_type: String             // "text_similar", "image_cluster", etc.
file_paths: List<String>       // Files in this similarity group
metadata: String               // JSON blob for group-specific data
created_at: Datetime           // When group was created
similarity_threshold: Float64  // Threshold used for grouping
```

#### Pairwise Relations
```rust
path_a: String                 // First file path
path_b: String                 // Second file path
relation_type: String          // "text_diff", "image_distance", etc.
score: Float64                 // Similarity score
data: String                   // JSON blob for relation-specific data
computed_at: Datetime          // When relationship was computed
```

## 4. System Architecture

### 4.1 System Interface

All processing systems implement a common interface:

```rust
#[async_trait::async_trait]
pub trait SystemRunner: Send + Sync {
    async fn run(&self, state: &ScanState, memory_mgr: &MemoryManager) -> PolarsResult<()>;
    fn can_run(&self, state: &ScanState) -> bool;
    fn priority(&self) -> u8;
}

pub trait System {
    fn required_columns(&self) -> &[&'static str];
    fn optional_columns(&self) -> &[&'static str];
}
```

### 4.2 Core Systems

#### FileDiscoverySystem
- **Purpose**: Discovers files in the filesystem and populates initial metadata
- **Required Columns**: None (creates initial data)
- **Output**: Populates `path`, `size`, `modified_ns`, `file_type`

#### ContentHashSystem
- **Purpose**: Computes content hashes for files
- **Required Columns**: `path`, `file_type`, `content_loaded`
- **Output**: Populates `blake3_hash`, `perceptual_hash`, `text_hash`, sets `hashed = true`

#### HashRelationSystem
- **Purpose**: Groups files by hash values into relations
- **Required Columns**: `path`, `blake3_hash`, `perceptual_hash`, `text_hash`
- **Output**: Populates hash relations table

#### TextSimilaritySystem
- **Purpose**: Finds groups of similar text files
- **Required Columns**: `path`, `file_type`, `content_loaded`
- **Output**: Creates similarity groups for text files

#### ImageSimilaritySystem
- **Purpose**: Finds groups of similar images using perceptual hashing
- **Required Columns**: `path`, `file_type`, `perceptual_hash`
- **Output**: Creates similarity groups for images

#### ExactDuplicateSystem
- **Purpose**: Identifies exact duplicates using content hashes
- **Required Columns**: `path`, `blake3_hash`
- **Output**: Creates exact duplicate groups

### 4.3 System Scheduler

The `SystemScheduler` orchestrates system execution:

```rust
pub struct SystemScheduler {
    systems: Vec<Box<dyn SystemRunner>>,
    state: ScanState,
    memory_mgr: MemoryManager,
}
```

**Scheduling Algorithm**:
1. Query each system to determine if it can run (`can_run()`)
2. Sort runnable systems by priority
3. Execute systems in parallel where data dependencies allow
4. Yield control between cycles for responsiveness
5. Repeat until no systems have work to do

## 5. Memory Management

### 5.1 Memory Manager

```rust
pub struct MemoryManager {
    max_bytes: usize,                                    // Total memory limit
    current_bytes: Arc<AtomicUsize>,                    // Current usage
    file_cache: LruCache<PathBuf, FileContents>,        // LRU cache (lock-free optimizations welcome)
}
```

### 5.2 Memory Strategy

- **Configurable limits**: Based on system memory via `Settings::from_sysinfo()`
- **LRU caching**: Frequently accessed files stay in memory
- **Lazy loading**: Files loaded only when needed by systems
- **Automatic eviction**: When memory pressure detected, evict LRU files
- **Streaming processing**: Large files processed in chunks when possible

### 5.3 Settings Configuration

```rust
pub struct Settings {
    pub max_total_loaded_bytes: usize,  // Half of available system memory
    pub num_max_loaded_files: usize,    // One per CPU core
}
```

## 6. Similarity Detection Architecture

### 6.1 Multi-Tier Similarity

The system implements a three-tier similarity detection strategy:

#### Tier 1: Exact Matches (Highest Priority)
- **Method**: Blake3 content hashing
- **Storage**: Hash relations table indexed by hash value
- **Use Case**: Identical files, even if renamed or moved

#### Tier 2: Content-Aware Similarity
- **Text Files**: Diff-based similarity using `similar` crate
- **Images**: Perceptual hashing for visual similarity
- **Storage**: Similarity groups table with group-based indexing

#### Tier 3: Metadata-Based Similarity
- **Method**: Filename patterns, size ranges, modification times
- **Storage**: Similarity groups with metadata-based grouping
- **Use Case**: Files that may be related but not content-identical

### 6.2 Pluggable Similarity Providers

```rust
#[async_trait::async_trait]
pub trait SimilarityProvider: Send + Sync {
    fn similarity_type(&self) -> &'static str;
    fn priority(&self) -> u8;
    
    async fn compute_similarity(&self, file_a: &FileRecord, file_b: &FileRecord) -> Option<f64>;
    fn can_compare(&self, file_a: &FileRecord, file_b: &FileRecord) -> bool;
}
```

**Built-in Providers**:
- `ExactHashProvider`: Blake3 hash comparison
- `PerceptualImageProvider`: Image perceptual hashing
- `TextDiffProvider`: Text content diffing
- `MetadataProvider`: Filename and size-based similarity

## 7. Query System

### 7.1 Query Interface

```rust
pub struct Query<'a> {
    data: &'a DataFrame,
    relations: &'a RelationStore,
}
```

### 7.2 Query Capabilities

#### File Queries
- Files by type, size range, modification time
- Files needing processing by specific systems
- Files with specific "components" (hashes, content loaded, etc.)

#### Relationship Queries
- Files with specific hash values (value-indexed)
- All duplicate groups by hash type
- Similarity groups by type (group-indexed)
- All groups containing a specific file
- Pairwise relationship data

#### Composite Queries
- All files similar to a given file across multiple similarity types
- Duplicate groups with configurable similarity thresholds
- Files that are duplicates in one dimension but unique in another

## 8. Client Interface Design

### 8.1 CLI Client (Pipelined)

The CLI client is designed for automation and advanced scripting purposes:

```rust
// Supports streaming output for pipeline integration
pub struct CliClient {
    detector: DuplicateDetector,
    output_format: OutputFormat, // JSON, CSV, TSV, etc.
}

impl CliClient {
    pub async fn scan_and_stream(&mut self, paths: Vec<PathBuf>) -> impl Stream<Item = ScanEvent>;
    pub async fn query_duplicates(&self, query: &str) -> Result<impl Stream<Item = DuplicateGroup>, CliError>;
    pub fn export_results(&self, format: OutputFormat) -> Result<String, CliError>;
}
```

**Key Features**:
- Streaming output for real-time processing in pipelines
- Multiple output formats (JSON, CSV, TSV) for integration
- Scriptable query interface with SQL-like syntax
- Batch processing capabilities for large datasets
- Nushell integration built on existing polars plugin

### 8.2 GUI Client (Responsive)

The GUI client prioritizes responsiveness with lazy-loaded table views:

```rust
pub struct GuiClient {
    detector: Arc<DuplicateDetector>,
    table_manager: LazyTableManager,
    update_channel: Receiver<UiUpdate>,
}

pub struct LazyTableManager {
    visible_range: Range<usize>,
    total_rows: usize,
    cached_rows: LruCache<usize, TableRow>,
    sort_column: Option<String>,
    filter_predicate: Option<String>,
}
```

**Key Features**:
- Lazy-loaded table views that only render visible rows
- Real-time updates without blocking the UI thread
- Responsive sorting and filtering with incremental updates
- Progress indicators with cancellation support

### 8.3 TUI Client (Responsive)

The TUI client provides a terminal-based interface with similar responsiveness:

```rust
pub struct TuiClient {
    detector: Arc<DuplicateDetector>,
    screen_manager: ScreenManager,
    input_handler: InputHandler,
}
```

**Key Features**:
- Keyboard-driven navigation optimized for terminal use
- Lazy table rendering similar to GUI client
- Real-time progress display with ASCII progress bars
- Efficient screen updates to minimize terminal flicker

## 9. Disk Caching Strategy

### 9.1 Cache Architecture

The system implements intelligent disk caching to avoid recomputing results:

```rust
pub struct CacheManager {
    cache_dir: PathBuf,
    metadata_cache: MetadataCache,
    dataframe_cache: DataFrameCache,
    filesystem_monitor: FilesystemMonitor,
}

pub struct MetadataCache {
    scan_metadata: HashMap<PathBuf, ScanMetadata>,
    file_checksums: HashMap<PathBuf, FileChecksum>,
}

pub struct ScanMetadata {
    scan_id: u32,
    last_scan_time: SystemTime,
    file_count: usize,
    directory_tree_hash: String, // Hash of directory structure
}
```

### 9.2 Cache Invalidation Strategy

**Filesystem Change Detection**:
- Directory tree hashing to detect structural changes
- File modification time and size tracking
- Incremental scanning for changed files only
- Efficient diff computation between scan states

**Cache Validation Process**:
1. Compare directory tree hash with cached version
2. Check modification times for files in cache
3. Identify added, removed, or modified files
4. Invalidate cache entries for changed files only
5. Preserve valid cache entries to minimize recomputation

### 9.3 Incremental Updates

```rust
impl CacheManager {
    pub async fn load_or_create_cache(&mut self, scan_paths: &[PathBuf]) -> Result<CacheLoadResult, CacheError>;
    pub async fn update_cache_incremental(&mut self, changes: &FilesystemChanges) -> Result<(), CacheError>;
    pub fn compute_cache_validity(&self, scan_paths: &[PathBuf]) -> CacheValidityReport;
}

pub struct CacheLoadResult {
    cached_data: Option<DataFrame>,
    invalidated_paths: Vec<PathBuf>,
    cache_hit_ratio: f64,
}
```

**Benefits**:
- Dramatically reduced startup time for repeated scans
- Efficient handling of large, mostly-unchanged directory trees
- Graceful degradation when cache is partially invalid
- Configurable cache retention policies

## 10. API Design

### 10.1 Core API

```rust
pub struct DuplicateDetector {
    state: ScanState,
    scheduler: SystemScheduler,
    memory_mgr: MemoryManager,
}

impl DuplicateDetector {
    pub fn new(config: DetectorConfig) -> Result<Self, DetectorError>;
    pub async fn scan_directory(&mut self, path: PathBuf) -> Result<ScanResults, DetectorError>;
    pub async fn process_until_complete(&mut self) -> PolarsResult<()>;
    pub fn query(&self) -> Query;
    pub fn raw_data(&self) -> &DataFrame;

    // Persistence operations for caching dataframes to disk
    pub async fn save_cache(&self, cache_path: &Path) -> Result<(), DetectorError>;
    pub async fn load_cache(&mut self, cache_path: &Path) -> Result<bool, DetectorError>;
    pub fn is_cache_valid(&self, cache_path: &Path) -> Result<bool, DetectorError>;
}
```

### 10.2 High-Level Operations

```rust
impl DuplicateDetector {
    pub fn find_exact_duplicates(&self) -> PolarsResult<Vec<DuplicateGroup>>;
    pub fn find_similar_groups(&self, group_type: &str) -> PolarsResult<Vec<SimilarityGroup>>;
    pub fn find_duplicates_of_file(&self, path: &str) -> PolarsResult<Vec<String>>;
    pub fn get_file_relationships(&self, path: &str) -> PolarsResult<FileRelationships>;
}
```

### 10.3 Progress Monitoring

```rust
pub struct ScanResults {
    progress_handle: ProgressHandle,
}

impl ScanResults {
    pub fn progress(&self) -> ScanProgress;
    pub fn is_complete(&self) -> bool;
    pub async fn wait_for_completion(&self) -> PolarsResult<()>;
}
```

## 11. Async Processing Pipeline

### 11.1 Pipeline Architecture

```
File Discovery → Content Processing → Hash Computation → Similarity Analysis → Relation Building
     (1)              (N)                  (N)                (N)                  (1)

Where:
(1) = Single-threaded stage
(N) = Multi-threaded stage (up to num_max_loaded_files)
```

### 11.2 Channel-Based Communication

```rust
async fn run_pipeline(pipeline: ProcessingPipeline) {
    let (file_tx, file_rx) = smol::channel::bounded(100);
    let (processed_tx, processed_rx) = smol::channel::bounded(100);

    // Stage 1: File discovery (single producer)
    let discovery_task = smol::spawn(discover_files(walker, file_tx));

    // Stage 2: Content processing (multiple workers)
    let processing_tasks = (0..num_workers)
        .map(|_| smol::spawn(process_files(file_rx.clone(), processed_tx.clone())))
        .collect::<Vec<_>>();

    // Stage 3: DataFrame updates (single consumer)
    let update_task = smol::spawn(update_dataframes(processed_rx, state));

    futures::join!(discovery_task, futures::future::join_all(processing_tasks), update_task);
}
```

### 11.3 Backpressure Management

- **Bounded channels**: Prevent memory exhaustion
- **Yield points**: Regular `smol::future::yield_now()` calls
- **Memory monitoring**: Pause processing when memory limits approached
- **Priority queuing**: High-priority files processed first

## 12. Error Handling

### 12.1 Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum DetectorError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("Memory limit exceeded")]
    MemoryExhausted,

    #[error("Invalid file type for operation: {0}")]
    InvalidFileType(String),

    #[error("Cache error: {0}")]
    Cache(#[from] CacheError),
}
```

### 12.2 Error Recovery

- **Graceful degradation**: Skip problematic files, continue processing
- **Logging**: Comprehensive error logging with `tracing`
- **Partial results**: Return partial results even if some operations fail
- **Retry logic**: Automatic retry for transient failures
- **Cache recovery**: Fallback to full scan when cache is corrupted

## 13. Performance Considerations

### 13.1 Polars Optimizations

- **Lazy evaluation**: Use LazyFrame for complex queries
- **Columnar operations**: Batch updates for efficiency
- **Memory mapping**: For large DataFrames that exceed memory
- **Predicate pushdown**: Filter early in query pipeline

### 13.2 I/O Optimizations

- **Async I/O**: Non-blocking file operations with smol
- **Read-ahead**: Predictive file loading based on scan patterns
- **Compression**: Optional compression for cached file contents
- **Parallel processing**: Multiple files processed simultaneously

### 13.3 Memory Optimizations

- **Streaming**: Process large files in chunks
- **Lazy loading**: Load file contents only when needed
- **Smart caching**: LRU eviction with size-aware policies
- **Memory pooling**: Reuse allocated buffers where possible
- **Lock-free algorithms**: Preferred over `Arc<RwLock<...>>` where possible

### 13.4 Cache Optimizations

- **Incremental updates**: Only reprocess changed files
- **Compressed storage**: Efficient disk usage for cached DataFrames
- **Parallel cache loading**: Load cache data concurrently with filesystem scanning
- **Smart invalidation**: Minimize cache misses through intelligent change detection

## 14. Testing Strategy

### 14.1 Unit Testing

- **System isolation**: Each system tested independently
- **Mock data**: Synthetic DataFrames for testing
- **Property testing**: Verify invariants across different inputs
- **Performance testing**: Benchmark critical paths
- **Cache testing**: Verify cache invalidation and loading logic

### 14.2 Integration Testing

- **End-to-end workflows**: Complete scan and duplicate detection
- **Multi-client testing**: Verify API works across CLI/GUI/TUI
- **Large dataset testing**: Performance with realistic file sets
- **Memory pressure testing**: Behavior under memory constraints
- **Cache integration**: Test cache persistence across application restarts

### 14.3 Benchmarking

- **Throughput**: Files processed per second
- **Memory efficiency**: Peak memory usage vs. dataset size
- **Query performance**: Response times for common queries
- **Scalability**: Performance across different dataset sizes
- **Cache performance**: Startup time improvements with cached data
- **UI responsiveness**: Frame rates and input latency for GUI/TUI

## 15. Future Extensions

### 15.1 Additional Similarity Providers

- **Audio fingerprinting**: For music duplicate detection
- **Video analysis**: Frame-based similarity for videos
- **Document similarity**: Semantic analysis for text documents
- **Archive content**: Similarity based on archive contents

### 15.2 Advanced Features

- **Distributed processing**: Scale across multiple machines
- **Machine learning**: Learn user preferences for duplicate handling
- **Cloud storage**: Support for cloud-based file systems
- **Watch mode**: Real-time monitoring of filesystem changes

### 15.3 UI Enhancements

- **Interactive filtering**: Dynamic query building
- **Batch operations**: Bulk duplicate resolution
- **Visualization**: Graphical representation of file relationships
- **Customizable layouts**: User-configurable interface arrangements

## 16. Conclusion

This design provides a robust, scalable foundation for duplicate file detection that:

- **Scales efficiently** with dataset size through columnar operations and async processing
- **Remains responsive** through careful memory management, yielding, and lazy UI updates
- **Supports extension** via pluggable similarity providers and system architecture
- **Provides flexibility** through comprehensive querying capabilities and multiple client interfaces
- **Maintains performance** through data-oriented design, Polars optimizations, and intelligent caching
- **Minimizes recomputation** through persistent disk caching with smart invalidation
- **Optimizes for different use cases** with specialized CLI (pipelined), GUI (responsive), and TUI (responsive) clients

The ECS-inspired architecture ensures that new functionality can be added without disrupting existing systems, while the relational data model provides efficient access patterns for both exact and similarity-based duplicate detection. The disk caching strategy dramatically improves startup performance for repeated scans, and the client-specific optimizations ensure each interface is tailored to its intended use case.

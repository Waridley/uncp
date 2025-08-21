//! Example demonstrating the type-safe relation system
//!
//! This example shows how to use the new RelationStore with type-safe keys
//! to store and retrieve different types of file relationships.

use polars::prelude::*;
use uncp::relations::{IdenticalHashes, RelationKey, RelationStore, SameFileName, SameSize};

// Define a custom relation type
struct SimilarImages;

impl RelationKey for SimilarImages {
	fn name() -> &'static str {
		"similar_images"
	}

	fn description() -> &'static str {
		"Images with similar visual content based on perceptual hashing"
	}

	fn create_schema() -> PolarsResult<DataFrame> {
		let file_paths =
			ListChunked::full_null_with_dtype("file_paths".into(), 0, &DataType::String);
		DataFrame::new(vec![
			Series::new("perceptual_hash".into(), Vec::<String>::new()).into(),
			Series::new("similarity_threshold".into(), Vec::<f64>::new()).into(),
			file_paths.into_series().into(),
			Series::new("confidence_score".into(), Vec::<f64>::new()).into(),
			Series::new("detected_at".into(), Vec::<i64>::new()).into(),
		])
	}
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("🔗 Type-Safe Relations Example");
	println!("==============================\n");

	// Create a new relation store
	let mut store = RelationStore::new();
	println!("✅ Created empty RelationStore");

	// Add some identical hash relations
	println!("\n📁 Adding identical hash relations...");
	let hash_paths_series = Series::new(
		"file_paths".into(),
		vec![
			Series::new("".into(), vec!["photo1.jpg", "photo1_copy.jpg"]),
			Series::new("".into(), vec!["document.pdf", "backup/document.pdf"]),
		],
	);

	let hash_data = DataFrame::new(vec![
		Series::new("hash_value".into(), vec!["abc123def456", "789xyz012abc"]).into(),
		Series::new("hash_type".into(), vec!["blake3", "blake3"]).into(),
		hash_paths_series.into(),
		Series::new("first_seen".into(), vec![1234567890i64, 1234567891i64]).into(),
		Series::new("file_count".into(), vec![2u32, 2u32]).into(),
	])?;

	store.insert::<IdenticalHashes>(hash_data)?;
	println!(
		"   ✓ Added {} identical hash groups",
		store.get::<IdenticalHashes>().unwrap().height()
	);

	// Add same filename relations
	println!("\n📄 Adding same filename relations...");
	let filename_paths_series = Series::new(
		"file_paths".into(),
		vec![Series::new(
			"".into(),
			vec!["config.json", "backup/config.json", "old/config.json"],
		)],
	);

	let filename_data = DataFrame::new(vec![
		Series::new("filename".into(), vec!["config.json"]).into(),
		filename_paths_series.into(),
		Series::new("file_count".into(), vec![3u32]).into(),
		Series::new("first_seen".into(), vec![1234567892i64]).into(),
	])?;

	store.insert::<SameFileName>(filename_data)?;
	println!(
		"   ✓ Added {} filename groups",
		store.get::<SameFileName>().unwrap().height()
	);

	// Add same size relations
	println!("\n📏 Adding same size relations...");
	let size_paths_series = Series::new(
		"file_paths".into(),
		vec![
			Series::new("".into(), vec!["small1.txt", "small2.txt"]),
			Series::new("".into(), vec!["large1.bin", "large2.bin", "large3.bin"]),
		],
	);

	let size_data = DataFrame::new(vec![
		Series::new("size_bytes".into(), vec![1024u64, 1048576u64]).into(),
		size_paths_series.into(),
		Series::new("file_count".into(), vec![2u32, 3u32]).into(),
		Series::new("first_seen".into(), vec![1234567893i64, 1234567894i64]).into(),
	])?;

	store.insert::<SameSize>(size_data)?;
	println!(
		"   ✓ Added {} size groups",
		store.get::<SameSize>().unwrap().height()
	);

	// Add custom relation type
	println!("\n🖼️  Adding custom similar images relation...");
	let image_paths_series = Series::new(
		"file_paths".into(),
		vec![Series::new(
			"".into(),
			vec!["sunset1.jpg", "sunset2.jpg", "sunset3.jpg"],
		)],
	);

	let image_data = DataFrame::new(vec![
		Series::new("perceptual_hash".into(), vec!["phash_abc123"]).into(),
		Series::new("similarity_threshold".into(), vec![0.85f64]).into(),
		image_paths_series.into(),
		Series::new("confidence_score".into(), vec![0.92f64]).into(),
		Series::new("detected_at".into(), vec![1234567895i64]).into(),
	])?;

	store.insert::<SimilarImages>(image_data)?;
	println!(
		"   ✓ Added {} similar image groups",
		store.get::<SimilarImages>().unwrap().height()
	);

	// Demonstrate type-safe retrieval
	println!("\n🔍 Type-safe retrieval examples:");

	// Check what relations exist
	println!("   Relations in store:");
	println!(
		"   - IdenticalHashes: {}",
		store.contains::<IdenticalHashes>()
	);
	println!("   - SameFileName: {}", store.contains::<SameFileName>());
	println!("   - SameSize: {}", store.contains::<SameSize>());
	println!("   - SimilarImages: {}", store.contains::<SimilarImages>());

	// Get metadata for each relation
	println!("\n📊 Relation metadata:");
	for metadata in store.all_metadata() {
		println!(
			"   {} ({}): {} rows, updated at {}",
			metadata.name,
			metadata.description,
			metadata.row_count,
			metadata.updated_at.format("%Y-%m-%d %H:%M:%S")
		);
	}

	// Demonstrate type safety - this won't compile:
	// let wrong_type = store.get::<String>(); // ❌ Compile error!

	// But this works perfectly:
	if let Some(hashes) = store.get::<IdenticalHashes>() {
		println!("\n🔐 Identical hash relations:");
		println!("{}", hashes);
	}

	if let Some(filenames) = store.get::<SameFileName>() {
		println!("\n📝 Same filename relations:");
		println!("{}", filenames);
	}

	// Demonstrate merging stores
	println!("\n🔄 Demonstrating store merging...");
	let mut store2 = RelationStore::new();

	// Add more hash data to second store
	let more_hash_paths = Series::new(
		"file_paths".into(),
		vec![Series::new(
			"".into(),
			vec!["video1.mp4", "video1_backup.mp4"],
		)],
	);

	let more_hash_data = DataFrame::new(vec![
		Series::new("hash_value".into(), vec!["def456ghi789"]).into(),
		Series::new("hash_type".into(), vec!["blake3"]).into(),
		more_hash_paths.into(),
		Series::new("first_seen".into(), vec![1234567896i64]).into(),
		Series::new("file_count".into(), vec![2u32]).into(),
	])?;

	store2.insert::<IdenticalHashes>(more_hash_data)?;

	println!(
		"   Store 1 has {} hash relations",
		store.get::<IdenticalHashes>().unwrap().height()
	);
	println!(
		"   Store 2 has {} hash relations",
		store2.get::<IdenticalHashes>().unwrap().height()
	);

	// Merge stores
	store.merge_with(&store2)?;

	println!(
		"   After merge: {} hash relations",
		store.get::<IdenticalHashes>().unwrap().height()
	);

	println!("\n✅ Type-safe relations example completed successfully!");
	println!("   Total relations stored: {}", store.len());

	Ok(())
}

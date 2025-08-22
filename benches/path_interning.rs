use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use polars::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use uncp::paths::{DirEntryId, intern_path};

/// Estimate memory usage of a data structure
fn estimate_memory_usage<T>(data: &[T]) -> usize {
	std::mem::size_of_val(data) + std::mem::size_of::<Vec<T>>()
}

/// Estimate memory usage of strings including heap allocations
fn estimate_string_memory(strings: &[String]) -> usize {
	let vec_overhead = std::mem::size_of::<Vec<String>>();
	let string_overhead = std::mem::size_of_val(strings);
	let heap_usage: usize = strings.iter().map(|s| s.capacity()).sum();
	vec_overhead + string_overhead + heap_usage
}

/// Print memory usage comparison
fn print_memory_comparison(size: usize) {
	println!("\n=== Memory Usage Comparison (n={}) ===", size);

	let paths = generate_test_paths(size);

	// Measure string storage
	let strings: Vec<String> = paths
		.iter()
		.map(|p| p.to_string_lossy().to_string())
		.collect();
	let string_memory = estimate_string_memory(&strings);

	// Measure interned storage
	let interned: Vec<DirEntryId> = paths.iter().map(intern_path).collect();
	let interned_memory = estimate_memory_usage(&interned);

	println!(
		"String storage:   {} bytes ({:.2} KB)",
		string_memory,
		string_memory as f64 / 1024.0
	);
	println!(
		"Interned storage: {} bytes ({:.2} KB)",
		interned_memory,
		interned_memory as f64 / 1024.0
	);
	println!(
		"Memory ratio:     {:.2}x (interned vs strings)",
		interned_memory as f64 / string_memory as f64
	);
	println!(
		"Space savings:    {:.1}%",
		(1.0 - interned_memory as f64 / string_memory as f64) * 100.0
	);
}

/// Generate a random hash string (like git SHA)
fn generate_hash(rng: &mut StdRng, length: usize) -> String {
	const CHARS: &[u8] = b"0123456789abcdef";
	(0..length)
		.map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
		.collect()
}

/// Generate diverse path patterns with varying characteristics
#[derive(Clone, Copy)]
pub enum PathPattern {
	Simple,    // Basic ASCII paths
	Deep,      // Deeply nested paths
	Unicode,   // Paths with Unicode characters
	HashBased, // Git-like hash directories
	Mixed,     // Combination of all patterns
}

/// Generate test paths with specified pattern and characteristics
fn generate_diverse_paths(count: usize, pattern: PathPattern, seed: u64) -> Vec<PathBuf> {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut paths = Vec::with_capacity(count);

	// Unicode directory and file names from various languages
	let unicode_dirs = [
		"文档",
		"图片",
		"下载", // Chinese
		"ドキュメント",
		"画像",
		"ダウンロード", // Japanese
		"문서",
		"사진",
		"다운로드", // Korean
		"Документы",
		"Изображения", // Russian
		"Έγγραφα",
		"Εικόνες", // Greek
		"مستندات",
		"صور", // Arabic
		"दस्तावेज़",
		"चित्र", // Hindi
	];

	let unicode_files = ["файл", "文件", "ファイル", "파일", "αρχείο", "ملف", "फ़ाइल"];

	// Base directories for different patterns
	let simple_bases = [
		"/home/user/Documents",
		"/home/user/Pictures",
		"/var/log",
		"/usr/local/bin",
		"/opt/software",
		"/tmp/build",
		"/etc/config",
		"/data/projects",
	];

	let deep_bases = [
		"/very/deeply/nested/directory/structure/with/many/levels",
		"/another/extremely/long/path/that/goes/very/deep/into/filesystem",
		"/projects/client/2024/quarter1/reports/monthly/detailed/analysis",
		"/backup/incremental/daily/2024/01/15/full/system/state/snapshots",
	];

	for i in 0..count {
		let path = match pattern {
			PathPattern::Simple => {
				let base = simple_bases[i % simple_bases.len()];
				let subdir = format!("subdir_{}", i % 10);
				let name = format!("file_{:06}.txt", i);
				PathBuf::from(base).join(subdir).join(name)
			}

			PathPattern::Deep => {
				let base = deep_bases[i % deep_bases.len()];
				let mut path = PathBuf::from(base);

				// Add random depth (5-15 additional levels)
				let depth = rng.gen_range(5..=15);
				for level in 0..depth {
					path = path.join(format!("level_{}", level));
				}

				let name = format!("deep_file_{:06}.dat", i);
				path.join(name)
			}

			PathPattern::Unicode => {
				let unicode_base = unicode_dirs[i % unicode_dirs.len()];
				let unicode_sub = unicode_dirs[(i + 1) % unicode_dirs.len()];
				let unicode_file = unicode_files[i % unicode_files.len()];

				let base_path = if i % 3 == 0 {
					format!("/home/用户/{}", unicode_base)
				} else {
					format!("/home/user/{}", unicode_base)
				};

				let name = format!("{}_{:04}.txt", unicode_file, i);
				PathBuf::from(base_path).join(unicode_sub).join(name)
			}

			PathPattern::HashBased => {
				// Git-like structure: .git/objects/ab/cdef123456...
				let hash = generate_hash(&mut rng, 40); // SHA-1 length
				let (prefix, suffix) = hash.split_at(2);

				let base_patterns = [
					format!("/.git/objects/{}/{}", prefix, suffix),
					format!(
						"/var/cache/artifacts/{}/{}/{}",
						&hash[0..2],
						&hash[2..4],
						&hash[4..]
					),
					format!("/tmp/build/{}/cache/{}", &hash[0..8], &hash[8..]),
					format!(
						"/data/blobs/{}/{}/{}/{}",
						&hash[0..2],
						&hash[2..4],
						&hash[4..6],
						&hash[6..]
					),
				];

				PathBuf::from(&base_patterns[i % base_patterns.len()])
			}

			PathPattern::Mixed => {
				// Randomly choose from other patterns
				let sub_pattern = match i % 4 {
					0 => PathPattern::Simple,
					1 => PathPattern::Deep,
					2 => PathPattern::Unicode,
					_ => PathPattern::HashBased,
				};
				let single_path = generate_diverse_paths(1, sub_pattern, seed + i as u64);
				paths.push(single_path.into_iter().next().unwrap());
				continue;
			}
		};

		paths.push(path);
	}

	paths
}

/// Legacy function for backward compatibility
fn generate_test_paths(count: usize) -> Vec<PathBuf> {
	generate_diverse_paths(count, PathPattern::Simple, 42)
}

/// Benchmark path interning vs string storage
fn bench_storage_memory(c: &mut Criterion) {
	let sizes = [100, 1000, 10000];

	for &size in &sizes {
		let paths = generate_test_paths(size);

		let mut group = c.benchmark_group("storage_memory");
		group.throughput(Throughput::Elements(size as u64));

		// Benchmark string storage
		group.bench_with_input(BenchmarkId::new("strings", size), &paths, |b, paths| {
			b.iter(|| {
				let mut string_storage: Vec<String> = Vec::new();
				for path in paths {
					string_storage.push(black_box(path.to_string_lossy().to_string()));
				}
				string_storage
			});
		});

		// Benchmark interned path storage
		group.bench_with_input(BenchmarkId::new("interned", size), &paths, |b, paths| {
			b.iter(|| {
				let mut interned_storage: Vec<DirEntryId> = Vec::new();
				for path in paths {
					interned_storage.push(black_box(intern_path(path)));
				}
				interned_storage
			});
		});

		group.finish();
	}
}

/// Benchmark lookup performance
fn bench_lookup_performance(c: &mut Criterion) {
	let sizes = [100, 1000, 10000];

	for &size in &sizes {
		let paths = generate_test_paths(size);

		// Pre-populate storage structures
		let string_map: HashMap<String, usize> = paths
			.iter()
			.enumerate()
			.map(|(i, path)| (path.to_string_lossy().to_string(), i))
			.collect();

		let interned_map: HashMap<DirEntryId, usize> = paths
			.iter()
			.enumerate()
			.map(|(i, path)| (intern_path(path), i))
			.collect();

		// Create lookup keys (subset of original paths)
		let lookup_keys: Vec<_> = paths.iter().step_by(10).collect();

		let mut group = c.benchmark_group("lookup_performance");
		group.throughput(Throughput::Elements(lookup_keys.len() as u64));

		// Benchmark string lookup
		group.bench_with_input(
			BenchmarkId::new("string_lookup", size),
			&lookup_keys,
			|b, keys| {
				b.iter(|| {
					let mut found = 0;
					for key in keys {
						let key_str = key.to_string_lossy().to_string();
						if string_map.contains_key(&key_str) {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark interned path lookup
		group.bench_with_input(
			BenchmarkId::new("interned_lookup", size),
			&lookup_keys,
			|b, keys| {
				b.iter(|| {
					let mut found = 0;
					for key in keys {
						let interned_key = intern_path(key);
						if interned_map.contains_key(&interned_key) {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		group.finish();
	}
}

/// Benchmark deduplication performance
fn bench_deduplication(c: &mut Criterion) {
	let sizes = [1000, 5000, 10000];

	for &size in &sizes {
		// Generate paths with many duplicates
		let mut paths = generate_test_paths(size / 4); // Generate 1/4 unique paths
		paths.extend(paths.clone()); // Duplicate them
		paths.extend(paths.clone()); // Duplicate again (4x total)

		let mut group = c.benchmark_group("deduplication");
		group.throughput(Throughput::Elements(paths.len() as u64));

		// Benchmark string deduplication
		group.bench_with_input(
			BenchmarkId::new("string_dedup", size),
			&paths,
			|b, paths| {
				b.iter(|| {
					let mut unique_strings: HashSet<String> = HashSet::new();
					for path in paths {
						unique_strings.insert(black_box(path.to_string_lossy().to_string()));
					}
					unique_strings.len()
				});
			},
		);

		// Benchmark interned path deduplication
		group.bench_with_input(
			BenchmarkId::new("interned_dedup", size),
			&paths,
			|b, paths| {
				b.iter(|| {
					let mut unique_interned: HashSet<DirEntryId> = HashSet::new();
					for path in paths {
						unique_interned.insert(black_box(intern_path(path)));
					}
					unique_interned.len()
				});
			},
		);

		group.finish();
	}
}

/// Benchmark display/formatting performance
fn bench_display_performance(c: &mut Criterion) {
	let sizes = [100, 1000, 5000];

	for &size in &sizes {
		let paths = generate_test_paths(size);
		let strings: Vec<String> = paths
			.iter()
			.map(|p| p.to_string_lossy().to_string())
			.collect();
		let interned: Vec<DirEntryId> = paths.iter().map(intern_path).collect();

		let mut group = c.benchmark_group("display_performance");
		group.throughput(Throughput::Elements(size as u64));

		// Benchmark string display
		group.bench_with_input(
			BenchmarkId::new("string_display", size),
			&strings,
			|b, strings| {
				b.iter(|| {
					let mut total_len = 0;
					for s in strings {
						total_len += black_box(s.to_string()).len();
					}
					total_len
				});
			},
		);

		// Benchmark interned path display
		group.bench_with_input(
			BenchmarkId::new("interned_display", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut total_len = 0;
					for id in interned {
						total_len += black_box(format!("{}", id)).len();
					}
					total_len
				});
			},
		);

		group.finish();
	}
}

/// Comprehensive memory analysis benchmark
fn bench_memory_analysis(c: &mut Criterion) {
	// Print detailed memory analysis for different sizes
	for &size in &[100, 1000, 10000] {
		print_memory_comparison(size);
	}

	// Benchmark memory allocation patterns
	let paths = generate_test_paths(1000);

	let mut group = c.benchmark_group("memory_allocation");

	// Benchmark repeated string allocation
	group.bench_function("string_allocation", |b| {
		b.iter(|| {
			let mut strings = Vec::new();
			for path in &paths {
				strings.push(black_box(path.to_string_lossy().to_string()));
			}
			strings
		});
	});

	// Benchmark repeated interning (should be faster due to caching)
	group.bench_function("interning_allocation", |b| {
		b.iter(|| {
			let mut interned = Vec::new();
			for path in &paths {
				interned.push(black_box(intern_path(path)));
			}
			interned
		});
	});

	group.finish();
}

/// Benchmark path component iteration
fn bench_path_iteration(c: &mut Criterion) {
	let paths = generate_test_paths(1000);
	let strings: Vec<String> = paths
		.iter()
		.map(|p| p.to_string_lossy().to_string())
		.collect();
	let interned: Vec<DirEntryId> = paths.iter().map(intern_path).collect();

	let mut group = c.benchmark_group("path_iteration");

	// Benchmark string path component counting
	group.bench_function("string_components", |b| {
		b.iter(|| {
			let mut total_components = 0;
			for s in &strings {
				let path = PathBuf::from(s);
				total_components += black_box(path.components().count());
			}
			total_components
		});
	});

	// Benchmark interned path component counting
	group.bench_function("interned_components", |b| {
		b.iter(|| {
			let mut total_components = 0;
			for id in &interned {
				total_components += black_box(id.iter().count());
			}
			total_components
		});
	});

	group.finish();
}

/// Benchmark Polars DataFrame operations with interned paths
fn bench_polars_dataframes(c: &mut Criterion) {
	let sizes = [1000, 5000, 10000];

	for &size in &sizes {
		let paths = generate_diverse_paths(size, PathPattern::Mixed, 42);

		// Create string-based DataFrame
		let string_paths: Vec<String> = paths
			.iter()
			.map(|p| p.to_string_lossy().to_string())
			.collect();
		let string_df = df! {
			"path" => string_paths.clone(),
			"size" => (0..size).map(|i| i as i64 * 1024).collect::<Vec<_>>(),
			"modified" => (0..size).map(|i| i as i64).collect::<Vec<_>>(),
		}
		.unwrap();

		// Create interned path-based DataFrame
		let interned_paths: Vec<DirEntryId> = paths.iter().map(intern_path).collect();
		let interned_values: Vec<AnyValue> = interned_paths.iter().map(|id| (*id).into()).collect();
		let interned_df = df! {
			"path" => interned_values,
			"size" => (0..size).map(|i| i as i64 * 1024).collect::<Vec<_>>(),
			"modified" => (0..size).map(|i| i as i64).collect::<Vec<_>>(),
		}
		.unwrap();

		let mut group = c.benchmark_group("polars_dataframes");
		group.throughput(Throughput::Elements(size as u64));

		// Benchmark DataFrame creation
		group.bench_with_input(
			BenchmarkId::new("string_df_creation", size),
			&string_paths,
			|b, paths| {
				b.iter(|| {
					let df = df! {
						"path" => paths.clone(),
						"size" => (0..size).map(|i| i as i64 * 1024).collect::<Vec<_>>(),
						"modified" => (0..size).map(|i| i as i64).collect::<Vec<_>>(),
					}
					.unwrap();
					black_box(df)
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("interned_df_creation", size),
			&interned_paths,
			|b, paths| {
				b.iter(|| {
					let values: Vec<AnyValue> = paths.iter().map(|id| (*id).into()).collect();
					let df = df! {
						"path" => values,
						"size" => (0..size).map(|i| i as i64 * 1024).collect::<Vec<_>>(),
						"modified" => (0..size).map(|i| i as i64).collect::<Vec<_>>(),
					}
					.unwrap();
					black_box(df)
				});
			},
		);

		// Benchmark DataFrame path filtering - simplified for now
		// Focus on basic operations that work reliably

		// Benchmark basic DataFrame filtering on numeric columns
		// This shows raw DataFrame performance when path columns are present

		group.bench_with_input(
			BenchmarkId::new("string_basic_filter", size),
			&string_df,
			|b, df| {
				b.iter(|| {
					// Filter on numeric columns to show basic DataFrame performance
					let result = df
						.clone()
						.lazy()
						.filter(col("size").gt(lit(1024)))
						.collect()
						.unwrap();
					black_box(result.height())
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("interned_basic_filter", size),
			&interned_df,
			|b, df| {
				b.iter(|| {
					// Filter on numeric columns to show basic DataFrame performance
					let result = df
						.clone()
						.lazy()
						.filter(col("size").gt(lit(1024)))
						.collect()
						.unwrap();
					black_box(result.height())
				});
			},
		);

		let sample_path = &string_paths[0];
		let interned_sample_path = intern_path(sample_path);

		group.bench_with_input(
			BenchmarkId::new("string_path_exact_filter", size),
			&(string_df.clone(), sample_path.clone()),
			|b, (df, target_path)| {
				b.iter(|| {
					let result = df
						.clone()
						.lazy()
						.filter(col("path").eq(lit(target_path.clone())))
						.collect()
						.unwrap();
					black_box(result.height())
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("interned_path_exact_filter", size),
			&(interned_df.clone(), interned_sample_path),
			|b, (df, target_path)| {
				b.iter(|| {
					let (idx, generation) = target_path.raw_parts();
					let result = df
						.clone()
						.lazy()
						.filter(col("path").struct_().field_by_index(0).eq(lit(idx as u64)))
						.filter(
							col("path")
								.struct_()
								.field_by_index(1)
								.eq(lit(generation as u64)),
						)
						.collect()
						.unwrap();
					black_box(result.height())
				});
			},
		);

		// Benchmark DataFrame joins (simulating file metadata joins)
		let join_size = size / 10; // Smaller join table
		let join_paths = &string_paths[0..join_size];
		let join_metadata: Vec<f64> = (0..join_size).map(|i| i as f64 * 0.5).collect();

		let string_join_df = df! {
			"path" => join_paths,
			"metadata" => join_metadata.clone(),
		}
		.unwrap();

		let join_interned: Vec<AnyValue> =
			join_paths.iter().map(|p| intern_path(p).into()).collect();

		let interned_join_df = df! {
			"path" => join_interned,
			"metadata" => join_metadata,
		}
		.unwrap();

		group.bench_with_input(
			BenchmarkId::new("string_df_join", size),
			&(string_df.clone(), string_join_df),
			|b, (main_df, join_df)| {
				b.iter(|| {
					let result = main_df
						.clone()
						.lazy()
						.join(
							join_df.clone().lazy(),
							[col("path")],
							[col("path")],
							JoinArgs::new(JoinType::Inner),
						)
						.collect()
						.unwrap();
					black_box(result)
				});
			},
		);

		// Test interned DataFrame join using struct field access
		// This should work by joining on the individual u64 components
		group.bench_with_input(
			BenchmarkId::new("interned_df_join", size),
			&(interned_df.clone(), interned_join_df),
			|b, (main_df, join_df)| {
				b.iter(|| {
					let result = main_df
						.clone()
						.lazy()
						.join(
							join_df.clone().lazy(),
							[
								col("path").struct_().field_by_name("idx"),
								col("path").struct_().field_by_name("gen"),
							],
							[
								col("path").struct_().field_by_name("idx"),
								col("path").struct_().field_by_name("gen"),
							],
							JoinArgs::new(JoinType::Inner),
						)
						.collect()
						.unwrap();
					black_box(result.height())
				});
			},
		);

		group.finish();
	}
}

/// Benchmark different path patterns
fn bench_path_patterns(c: &mut Criterion) {
	let patterns = [
		("simple", PathPattern::Simple),
		("deep", PathPattern::Deep),
		("unicode", PathPattern::Unicode),
		("hash_based", PathPattern::HashBased),
		("mixed", PathPattern::Mixed),
	];

	let size = 1000;

	for (name, pattern) in patterns {
		let paths = generate_diverse_paths(size, pattern, 42);

		let mut group = c.benchmark_group("path_patterns");
		group.throughput(Throughput::Elements(size as u64));

		// Benchmark string storage for this pattern
		group.bench_with_input(
			BenchmarkId::new(format!("string_{}", name), size),
			&paths,
			|b, paths| {
				b.iter(|| {
					let strings: Vec<String> = paths
						.iter()
						.map(|p| p.to_string_lossy().to_string())
						.collect();
					black_box(strings)
				});
			},
		);

		// Benchmark interned storage for this pattern
		group.bench_with_input(
			BenchmarkId::new(format!("interned_{}", name), size),
			&paths,
			|b, paths| {
				b.iter(|| {
					let interned: Vec<DirEntryId> = paths.iter().map(intern_path).collect();
					black_box(interned)
				});
			},
		);

		group.finish();
	}
}

/// Benchmark path comparison and lookup operations
fn bench_path_operations(c: &mut Criterion) {
	let sizes = [1000, 5000];

	for &size in &sizes {
		let paths = generate_diverse_paths(size, PathPattern::Mixed, 42);
		let strings: Vec<String> = paths
			.iter()
			.map(|p| p.to_string_lossy().to_string())
			.collect();
		let interned: Vec<DirEntryId> = paths.iter().map(intern_path).collect();

		// Create lookup targets (10% of the dataset)
		let lookup_targets_str: Vec<String> = strings.iter().step_by(10).cloned().collect();
		let lookup_targets_interned: Vec<DirEntryId> =
			interned.iter().step_by(10).cloned().collect();

		let mut group = c.benchmark_group("path_operations");
		group.throughput(Throughput::Elements(lookup_targets_str.len() as u64));

		// Benchmark exact string equality
		group.bench_with_input(
			BenchmarkId::new("string_equality", size),
			&(strings.clone(), lookup_targets_str.clone()),
			|b, (haystack, needles)| {
				b.iter(|| {
					let mut found = 0;
					for needle in needles {
						for hay in haystack {
							if hay == needle {
								found += 1;
								break;
							}
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark exact interned path equality (should be very fast - pointer comparison)
		group.bench_with_input(
			BenchmarkId::new("interned_equality", size),
			&(interned.clone(), lookup_targets_interned.clone()),
			|b, (haystack, needles)| {
				b.iter(|| {
					let mut found = 0;
					for needle in needles {
						for hay in haystack {
							if hay == needle {
								found += 1;
								break;
							}
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark string prefix matching
		group.bench_with_input(
			BenchmarkId::new("string_prefix_match", size),
			&strings,
			|b, strings| {
				b.iter(|| {
					let mut found = 0;
					for s in strings {
						if s.starts_with("/home/") || s.starts_with("/var/") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("interned_prefix_match_optimized", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						if id.is_descendant_of_path("/home/") || id.is_descendant_of_path("/var/") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("interned_prefix_match_resolved", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						let p = id.resolve();
						if p.starts_with(
							<str as AsRef<Path>>::as_ref("/home/")
								.canonicalize()
								.unwrap(),
						) || p.starts_with(
							<str as AsRef<Path>>::as_ref("/var/")
								.canonicalize()
								.unwrap(),
						) {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("string_descendant_of", size),
			&strings,
			|b, strings| {
				b.iter(|| {
					let mut found = 0;
					for s in strings {
						let s = std::path::absolute(PathBuf::from(s)).unwrap();
						if s.starts_with("/home/") || s.starts_with("/var/") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		let home = intern_path("/home/");
		let var = intern_path("/var/");
		group.bench_with_input(
			BenchmarkId::new("interned_descendant_of", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						if id.is_descendant_of(home) || id.is_descendant_of(var) {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark string glob-like matching
		group.bench_with_input(
			BenchmarkId::new("string_glob_match", size),
			&strings,
			|b, strings| {
				b.iter(|| {
					let mut found = 0;
					for s in strings {
						if s.contains(".txt") || s.contains(".jpg") || s.contains(".dat") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark interned path extension matching (optimized)
		group.bench_with_input(
			BenchmarkId::new("interned_extension_match_optimized", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						if id.has_extension("txt")
							|| id.has_extension("jpg")
							|| id.has_extension("dat")
						{
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark interned path name contains (optimized - no allocation)
		group.bench_with_input(
			BenchmarkId::new("interned_name_contains_optimized", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						if id.name_contains("file") || id.name_contains("test") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark interned path glob-like matching (old method for comparison)
		group.bench_with_input(
			BenchmarkId::new("interned_glob_match_string", size),
			&interned,
			|b, interned| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						let s = format!("{}", id);
						if s.contains(".txt") || s.contains(".jpg") || s.contains(".dat") {
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		// Benchmark interned path with compiled glob (most efficient for repeated patterns)
		let txt_matcher = globset::Glob::new("*.txt").unwrap().compile_matcher();
		let jpg_matcher = globset::Glob::new("*.jpg").unwrap().compile_matcher();
		let dat_matcher = globset::Glob::new("*.dat").unwrap().compile_matcher();

		group.bench_with_input(
			BenchmarkId::new("interned_compiled_glob_match", size),
			&(interned.clone(), txt_matcher, jpg_matcher, dat_matcher),
			|b, (interned, txt, jpg, dat)| {
				b.iter(|| {
					let mut found = 0;
					for id in interned {
						if id.name_matches_compiled_glob(txt)
							|| id.name_matches_compiled_glob(jpg)
							|| id.name_matches_compiled_glob(dat)
						{
							found += 1;
						}
					}
					black_box(found)
				});
			},
		);

		group.finish();
	}
}

criterion_group!(
	benches,
	bench_storage_memory,
	bench_lookup_performance,
	bench_deduplication,
	bench_display_performance,
	bench_memory_analysis,
	bench_path_iteration,
	bench_polars_dataframes,
	bench_path_patterns,
	bench_path_operations
);
criterion_main!(benches);

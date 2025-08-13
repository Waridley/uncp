# UNCP - Un-copy (Duplicate File Detection)

A tool to help you de-duplicate your data.

## Features

- **Multiple similarity metrics** such as exact content hashing, perceptual hashing, and text diffing
- **Asynchronous scanning** for responsive UI and immediate access to discovered files and relationships
- **High-performance file analysis** — speed is a top priority guiding development
- **Builds a graph of file relationships** for flexible querying
- **Data-oriented design** backed by Polars DataFrames and a custom system scheduler
- **Parquet cache format** allowing for analysis in other data analytics tools
- **Multiple ways to use**, including as a Rust library, CLI, GUI, and TUI.
- **Cross-platform support** (Linux, macOS, Windows)

## Installation

### From Source

Requires [the Rust toolchain](https://www.rust-lang.org/tools/install)

```bash
git clone https://github.com/Waridley/uncp.git
cd uncp
cargo install --path cli --release
```

### TODO -- Publish to crates.io
> ### Using Cargo
> 
> ```bash
> cargo install uncp-cli
> cargo install uncp-gui
> cargo install uncp-tui
> ```

## Usage

### Command Line Interface

```bash
# Scan a directory for duplicates
uncp scan /path/to/directory

# Scan and hash files for content-based detection
uncp scan /path/to/directory --hash

# Filter files using globs
uncp scan /path/to/directory --include "**/*.jpg" --include "**/*.png" --exclude "**/node_modules/**"

# Clear the cache to save data or if it has been corrupted somehow
uncp clear-cache
```

### Terminal User Interface

```bash
uncp-tui
```

### Graphical User Interface

```bash
uncp-gui
```

## License

All code in this repository is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.

## Acknowledgments

- Built with [Polars](https://pola.rs/) for efficient data processing
- Uses [Blake3](https://github.com/BLAKE3-team/BLAKE3) for fast, secure hashing
- GUI powered by [Iced](https://iced.rs/)
- TUI built with [Ratatui](https://ratatui.rs/)

## LLM Usage Disclaimer

The formal [Design Document](DESIGN.md) and initial implementation of this
project was effectively vibe-coded using the Augment Code plugin for CLion,
as a way for me to quickly get something working and test the limits of the
vibe coding approach myself. I may continue to use LLMs for some of the work
on this project, but not quite to the extent that I did for the initial work.

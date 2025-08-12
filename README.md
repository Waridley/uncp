# UNCP - Un-copy (Duplicate File Detection)

A comprehensive duplicate file detection and management tool built in Rust, featuring CLI, GUI, and TUI interfaces.

## Features

- **Fast file discovery** with configurable filters
- **Content-based duplicate detection** using Blake3 hashing
- **Multiple interfaces**: Command-line (CLI), Graphical (GUI), and Terminal (TUI)
- **Efficient caching** with Polars DataFrames
- **Memory-aware processing** with configurable limits
- **Cross-platform support** (Linux, macOS, Windows)

## Installation

### From Source

```bash
git clone https://github.com/your-username/uncp.git
cd uncp
cargo build --release
```

### Using Cargo

```bash
cargo install uncp-cli
```

## Usage

### Command Line Interface

```bash
# Scan a directory for duplicates
uncp scan /path/to/directory

# Scan and hash files for content-based detection
uncp scan /path/to/directory --hash

# Clear the cache
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

## Development

### Prerequisites

- Rust 1.70+ (stable toolchain)
- System dependencies:
  - Linux: `pkg-config`, `libssl-dev`
  - macOS: Xcode command line tools
  - Windows: Visual Studio Build Tools

### Building

```bash
# Build all workspace members
cargo build --workspace

# Build with optimizations
cargo build --workspace --release
```

### Testing

```bash
# Run all tests
cargo test --workspace

# Run tests with all features
cargo test --workspace --all-features

# Run benchmarks
cargo bench
```

### Code Quality

```bash
# Check formatting
cargo fmt --all -- --check

# Run clippy lints
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Security audit
cargo audit
```

## Continuous Integration

This project uses GitHub Actions for CI/CD with the following workflows:

### Main CI Pipeline (`.github/workflows/ci.yml`)

- **Multi-platform testing**: Ubuntu, Windows, macOS
- **Code quality checks**: formatting, clippy lints, security audit
- **Documentation building**: ensures docs compile correctly
- **Code coverage**: generates coverage reports with codecov

### Release Pipeline (`.github/workflows/release.yml`)

- **Automated releases**: triggered on version tags
- **Cross-platform binaries**: builds for multiple targets
- **Crates.io publishing**: publishes to the Rust package registry

### Benchmarking (`.github/workflows/benchmark.yml`)

- **Performance tracking**: runs benchmarks on main branch
- **Memory profiling**: uses Valgrind for memory usage analysis
- **Regression detection**: alerts on performance degradation

### Dependency Management

- **Dependabot**: automated dependency updates
- **Security scanning**: regular vulnerability checks

## Architecture

The project uses a modular, data-oriented design:

- **Core library** (`src/`): Main duplicate detection logic
- **CLI** (`cli/`): Command-line interface
- **GUI** (`gui/`): Graphical interface using Iced
- **TUI** (`tui/`): Terminal interface using Ratatui

### Key Components

- **Systems**: Modular processing pipeline (discovery, hashing, similarity)
- **Data layer**: Polars DataFrames for efficient data operations
- **Caching**: Persistent storage for scan results
- **Memory management**: Adaptive memory usage based on system resources

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure CI passes
6. Submit a pull request

### Code Style

- Follow Rust standard formatting (`cargo fmt`)
- Address all clippy warnings
- Add documentation for public APIs
- Include tests for new features

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Polars](https://pola.rs/) for efficient data processing
- Uses [Blake3](https://github.com/BLAKE3-team/BLAKE3) for fast, secure hashing
- GUI powered by [Iced](https://iced.rs/)
- TUI built with [Ratatui](https://ratatui.rs/)

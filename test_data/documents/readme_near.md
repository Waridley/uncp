# Test Data Documentation

This directory contains sample data for the UNCP (Universal Non-Duplicate Copy) project.

## Directory Structure

- `documents/` - Text documents, markdown files, and documentation
- `images/` - Image files of various formats and sizes
- `audio/` - Audio files in different formats
- `video/` - Video files for testing
- `archives/` - Compressed archives and packages
- `databases/` - Database files and data dumps
- `code/` - Source code files in various languages
- `logs/` - Log files of different sizes
- `configs/` - Configuration files
- `temp/` - Temporary files and cache data

## File Types Included

### Text Files
- Markdown (.md)
- Plain text (.txt)
- CSV data (.csv)
- JSON configuration (.json)
- YAML configuration (.yaml)
- XML data (.xml)
- Source code (.rs, .py, .js, .c, .cpp, .java)

### Binary Files
- Images (PNG, JPEG, GIF, BMP)
- Audio (MP3, WAV, FLAC)
- Video (MP4, AVI, MKV)
- Archives (ZIP, TAR.GZ, 7Z)
- Databases (SQLite, binary data)
- Executables and libraries

### Large Files
- Some files are intentionally large to test performance
- Duplicate files with different names to test deduplication
- Files with similar content but slight differences

## Usage

This sample data is designed to exercise various aspects of the duplicate detection system:

1. **File type detection** - Various file extensions and MIME types
2. **Size handling** - Files ranging from bytes to megabytes
3. **Duplicate detection** - Identical files with different names
4. **Performance testing** - Large files and many small files
5. **Path filtering** - Nested directory structures

## Notes

- Binary files are tracked with Git LFS
- Some files are intentionally duplicated for testing
- File sizes range from very small (< 1KB) to large (> 10MB)
- Various character encodings are used in text files

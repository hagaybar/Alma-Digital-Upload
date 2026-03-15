# Filename-Based Upload Workflow

This document describes the workflow for uploading digital files using MMS ID filename matching.

## Overview

The filename-based workflow is designed for collections where:
- Files are named with the MMS ID (e.g., `990012345678904146.pdf`)
- All files are in a single input folder
- One file per bibliographic record

## How It Works

1. **Set Retrieval**: Get MMS IDs from an Alma bibliographic set
2. **File Discovery**: Scan input folder for PDF files
3. **Matching**: Match files to records by MMS ID in filename
4. **Representation Creation**: Create digital representations
5. **Upload & Link**: Upload files to S3 and link to representations

## Prerequisites

### 1. Environment Variables

```bash
export ALMA_SB_API_KEY='your_sandbox_key'
export ALMA_PROD_API_KEY='your_production_key'
export AWS_ACCESS_KEY='your_aws_access_key'
export AWS_SECRET='your_aws_secret_key'
export ALMA_SB_BUCKET_NAME='your_sandbox_bucket'
export ALMA_PROD_BUCKET_NAME='your_production_bucket'
```

### 2. File Organization

Your files must be named with MMS IDs:

```
input_folder/
├── 990012345678901234.pdf
├── 990012345678905678.pdf
├── 990012345678909012.pdf
└── ...
```

**Important**: The filename must be exactly `{mms_id}.pdf`:
- ✓ `990012345678901234.pdf`
- ✗ `990012345678901234_v1.pdf`
- ✗ `book_990012345678901234.pdf`

### 3. Alma Set

Create a bibliographic set in Alma containing the MMS IDs to process.

## Configuration

Create a configuration file:

```json
{
    "alma": {
        "environment": "SANDBOX",
        "set_id": "35889159610004146",
        "library_code": "YOUR_LIBRARY_CODE",
        "access_rights_code": "",
        "access_rights_desc": ""
    },
    "matching": {
        "strategy": "mms-id-filename",
        "files_root": "/path/to/scans",
        "file_extension": "pdf"
    },
    "aws": {
        "institution_code": "YOUR_INSTITUTION_CODE"
    },
    "output_settings": {
        "output_directory": "./output"
    },
    "options": {
        "dry_run": true
    }
}
```

### Configuration Notes

- **library_code**: Must be the exact Alma library code
- **files_root**: Path to folder containing PDF files
- **file_extension**: Usually "pdf" but can be changed

## Usage

### Step 1: Pre-flight Check

Before running, verify:

1. **Alma Set Exists and Contains Expected Records**

2. **Files Are Named Correctly**:
   ```bash
   ls /path/to/input_folder/*.pdf | head
   ```

   All filenames should be numeric MMS IDs.

3. **File Count Matches**:
   ```bash
   ls /path/to/input_folder/*.pdf | wc -l
   ```

### Step 2: Dry Run

Always start with a dry run:

```bash
poetry run python alma_digital_upload.py \
    --config config/filename_config.json \
    --match-strategy mms-id-filename \
    --dry-run
```

Review the output:
- How many records from the set?
- How many files found?
- How many matched?
- Any unmatched records or files?

### Step 3: Step-by-Step Testing

Test individual steps:

```bash
# Step 1: Verify set retrieval
poetry run python alma_digital_upload.py --config config.json --step 1

# Step 2: Verify file matching
poetry run python alma_digital_upload.py --config config.json --step 2

# Step 3: Test representation creation
poetry run python alma_digital_upload.py --config config.json --step 3

# Step 4: Test upload process
poetry run python alma_digital_upload.py --config config.json --step 4
```

### Step 4: Live Run

When ready for production:

```bash
poetry run python alma_digital_upload.py \
    --config config/filename_config.json \
    --match-strategy mms-id-filename \
    --live
```

## S3 Path Structure

Files are uploaded with this path pattern:

```
{institution_code}/upload/{mms_id}/{filename}
```

Example:
```
YOUR_INST/upload/990012345678901234/990012345678901234.pdf
```

## Comparison with MARC 907$e Workflow

| Aspect | MMS ID Filename | MARC 907$e |
|--------|-----------------|------------|
| File naming | `{mms_id}.pdf` | Any filename |
| File location | Single folder | Multiple folders |
| Files per record | One | Multiple |
| MARC dependency | None | Requires 907$e field |
| Complexity | Simple | More complex |
| Use case | Simple PDF collection | Complex folder structures |

## Troubleshooting

### No Files Matched

**Symptom**: Files found but none matched to set members

**Solutions**:
1. Check that filenames are numeric MMS IDs
2. Verify MMS IDs in filenames match those in the set
3. Check for leading zeros or formatting differences

### Invalid Filename Format

**Symptom**: Files show as "invalid MMS ID format"

**Solutions**:
1. Remove any prefix/suffix from filenames
2. Ensure filename (without extension) is purely numeric
3. Rename files to match MMS ID exactly

### Records Without Matching Files

**Symptom**: Set has more records than matched files

**Solutions**:
1. Check which MMS IDs are missing files
2. Verify file extension matches configuration
3. Create missing files or update the set

### Files Not in Set

**Symptom**: Files exist but aren't processed

**Solutions**:
1. Files not in set won't be processed (by design)
2. Update the set to include these MMS IDs
3. Or create a new set that includes all files

## Best Practices

1. **Validate file naming** before starting
2. **Test with small sets** (5-10 records) first
3. **Use dry-run mode** before live operations
4. **Keep backups** of original files
5. **Review log files** after each run
6. **Compare counts**:
   - Set member count
   - File count
   - Matched count

## Example Workflow

```bash
# 1. Check file count
ls /path/to/pdfs/*.pdf | wc -l
# Output: 150

# 2. Check naming (should show numeric IDs)
ls /path/to/pdfs/*.pdf | head -5

# 3. Create config file
cp config/config.example.json config/my_config.json
# Edit config with your values

# 4. Dry run
poetry run python alma_digital_upload.py \
    --config config/my_config.json \
    --match-strategy mms-id-filename \
    --dry-run

# 5. Review output
# - Total records from set: 150
# - Files found: 145
# - Matched: 145
# - Records without files: 5

# 6. Investigate unmatched records
# Check which 5 MMS IDs don't have files

# 7. Live run (when ready)
poetry run python alma_digital_upload.py \
    --config config/my_config.json \
    --match-strategy mms-id-filename \
    --live

# 8. Verify in Alma
# Check that representations were created
# Check that files are linked
```

## Advanced Usage

### Finding Unmatched Files

After running, you can identify files that weren't in the set:

```python
from strategies import MmsIdFilenameStrategy

config = {"alma": {"library_code": "TEST"}, "matching": {"files_root": "/path"}}
strategy = MmsIdFilenameStrategy(config)

# After matching
matched_mms_ids = {r.mms_id for r in results if r.matched}
unmatched_files = strategy.get_unmatched_files(matched_mms_ids)

for file_path in unmatched_files:
    print(f"File not in set: {file_path}")
```

### Processing Multiple Extensions

If you have mixed file types:

```json
{
    "matching": {
        "strategy": "mms-id-filename",
        "files_root": "/path/to/files",
        "file_extension": "tif"
    }
}
```

Run separately for each extension, or rename files to a common extension.

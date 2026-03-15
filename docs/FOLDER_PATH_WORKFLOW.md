# Folder-Path Upload Workflow

This document describes the workflow for uploading digital files using MARC 907$e field matching.

## Overview

The folder-path workflow is designed for collections where:
- Files are organized in folders on the filesystem
- Folder paths are stored in MARC 907$e fields
- Multiple files may exist per bibliographic record

## How It Works

1. **Set Retrieval**: Get MMS IDs from an Alma bibliographic set
2. **MARC Extraction**: Extract 907$e values from each record
3. **Path Construction**: Build file paths: `files_root` + `907$e_value`
4. **File Discovery**: Find all files in matched folders
5. **Representation Creation**: Create digital representations
6. **Upload & Link**: Upload files to S3 and link to representations

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

Your files should be organized like this:

```
files_root/
├── path/from/907e/value1/
│   ├── file1.tif
│   ├── file2.tif
│   └── file3.tif
├── path/from/907e/value2/
│   ├── image1.jpg
│   └── image2.jpg
└── ...
```

The MARC 907$e field contains the path relative to `files_root`.

### 3. MARC Records

Your bibliographic records should have MARC 907 fields like:

```
907  $e path/from/907e/value1
907  $l optional_prefix_identifier
```

## Configuration

Create a configuration file:

```json
{
    "alma": {
        "environment": "SANDBOX",
        "set_id": "12345678900004146",
        "library_code": "YOUR_LIBRARY_CODE",
        "access_rights_code": "",
        "access_rights_desc": ""
    },
    "matching": {
        "strategy": "marc-907e",
        "files_root": "/path/to/your/files",
        "marc_field": "907",
        "marc_subfield": "e"
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

- **library_code**: Must be the exact Alma library code, not a batch identifier
- **files_root**: Use WSL format for Windows paths (`/mnt/c/...`)
- **marc_field**: Usually "907"
- **marc_subfield**: Usually "e" for path, "l" for identifier

## Usage

### Step 1: Pre-flight Check

Before running, verify:

1. **Alma Set Exists**:
   ```bash
   # The script will validate the set, but you can check manually
   ```

2. **Files Are Accessible**:
   ```bash
   ls -la "/path/to/your/files_root"
   ```

3. **MARC 907$e Values Match Folders**:
   - Use the MARC extraction utility to generate a mapping
   - Use the folder matching utility to verify paths exist

### Step 2: Dry Run

Always start with a dry run:

```bash
poetry run python alma_digital_upload.py \
    --config config/folder_workflow_config.json \
    --match-strategy marc-907e \
    --dry-run
```

Review the output:
- How many records were matched?
- Are the file paths correct?
- Are there any errors?

### Step 3: Step-by-Step Testing

Test individual steps:

```bash
# Step 1: Verify set retrieval
poetry run python alma_digital_upload.py --config config.json --step 1

# Step 2: Verify file matching
poetry run python alma_digital_upload.py --config config.json --step 2

# Step 3: Test representation creation (still dry-run)
poetry run python alma_digital_upload.py --config config.json --step 3

# Step 4: Test upload process (still dry-run)
poetry run python alma_digital_upload.py --config config.json --step 4
```

### Step 4: Live Run

When ready for production:

```bash
poetry run python alma_digital_upload.py \
    --config config/folder_workflow_config.json \
    --match-strategy marc-907e \
    --live
```

## S3 Path Structure

Files are uploaded with this path pattern:

```
{institution_code}/upload/{marc_907e_value}/{filename}
```

Example:
```
YOUR_INST/upload/collection/2024/item001/image001.tif
```

## Utility Tools

### MARC 907 Extraction

Extract MARC data to a TSV file:

```python
from utils import Marc907Extractor

config = {
    "processing": {
        "field": "907",
        "subfield_e": "e",
        "subfield_l": "l",
        "prefix_removal": {"enabled": True, "delimiter": "_"}
    },
    "output_settings": {
        "output_directory": "./output",
        "include_headers": True
    }
}

extractor = Marc907Extractor(config)
results = extractor.extract_from_mms_ids(mms_ids, bibs_client)
extractor.write_tsv_output(results)
```

### Folder Matching

Verify which TSV values have matching folders:

```python
from utils import FolderMatcher

matcher = FolderMatcher()
records = matcher.read_tsv_file("output/marc_907_mapping.tsv")
folders = matcher.list_folders("/path/to/files_root")
results, unmatched = matcher.match_records_to_folders(records, folders)
matcher.write_report(results, unmatched)
matcher.display_summary(results, unmatched)
```

### Folder Renaming

Rename folders from 907$l values to 907$e values:

```python
from utils import FolderRenamer

config = {
    "input": {
        "tsv_file": "folder_match_report.tsv",
        "folder_path": "/path/to/folders"
    },
    "processing": {
        "source_column": "907$l_cleaned",
        "target_column": "907$e",
        "match_filter": "Match_907l"
    },
    "output_settings": {"output_directory": "./output"},
    "dry_run": True  # Start with dry-run!
}

renamer = FolderRenamer(config)
# ... see full documentation
```

## Troubleshooting

### No MARC 907$e Found

**Symptom**: Records show "no_marc" status

**Solutions**:
1. Verify MARC field/subfield configuration
2. Check if records actually have 907$e fields
3. Run MARC extraction utility to see all values

### Path Does Not Exist

**Symptom**: Records show "path_not_found" status

**Solutions**:
1. Verify `files_root` path is correct
2. Check WSL path format (use `/mnt/c/...` not `C:/...`)
3. Verify 907$e values match actual folder names
4. Use folder matching utility to identify mismatches

### Library Code Invalid

**Symptom**: Representation creation fails with "invalid library code"

**Solutions**:
1. Library code must be exact Alma library code
2. Don't use batch identifiers
3. Verify in Alma Configuration → General → Libraries

### No Files Found in Folder

**Symptom**: Folder exists but no files to upload

**Solutions**:
1. Check folder contents manually
2. Verify files exist (not just subfolders)
3. Check file permissions

## Best Practices

1. **Always test in SANDBOX first**
2. **Use dry-run mode** before live operations
3. **Start with small test sets** (5-10 records)
4. **Review log files** after each run
5. **Backup important data** before live operations
6. **Run during off-peak hours** for large sets

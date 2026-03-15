# Alma Digital Upload

A unified tool for uploading digital files to Alma ILS (Integrated Library System) with support for multiple file matching strategies.

Part of TAU Libraries repositories.

## Overview

Alma Digital Upload provides a streamlined workflow for:
1. Retrieving bibliographic records from Alma sets
2. Matching local files to records using configurable strategies
3. Creating digital representations in Alma
4. Uploading files to AWS S3
5. Linking uploaded files to representations

## Features

- **Multiple Matching Strategies**: Choose between MARC field matching or filename-based matching
- **Dry-Run Mode**: Test your configuration without making actual changes
- **Step-by-Step Execution**: Run individual steps for testing and debugging
- **Comprehensive Logging**: Detailed logs for tracking progress and troubleshooting
- **Resume Support**: Resume interrupted uploads from where they stopped

## Installation

```bash
# Clone the repository
git clone https://github.com/hagaybar/Alma-Digital-Upload.git
cd Alma-Digital-Upload

# Install dependencies
poetry install
```

## Environment Variables

Set these environment variables before running:

```bash
# Alma API Keys
export ALMA_SB_API_KEY='your_sandbox_api_key'
export ALMA_PROD_API_KEY='your_production_api_key'

# AWS Credentials
export AWS_ACCESS_KEY='your_aws_access_key'
export AWS_SECRET='your_aws_secret_key'

# AWS S3 Bucket Names
export ALMA_SB_BUCKET_NAME='your_sandbox_bucket'
export ALMA_PROD_BUCKET_NAME='your_production_bucket'
```

## Configuration

Create a configuration file based on `config/config.example.json`:

```json
{
    "alma": {
        "environment": "SANDBOX",
        "set_id": "YOUR_SET_ID",
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
    "options": {
        "dry_run": true
    }
}
```

## Usage

### Basic Usage

```bash
# Dry-run mode (default - no actual changes)
poetry run python alma_digital_upload.py --config config.json

# Live mode (performs actual uploads)
poetry run python alma_digital_upload.py --config config.json --live
```

### Matching Strategies

#### MARC 907$e Strategy (Folder-Path Workflow)

Matches files by extracting path identifiers from MARC 907$e field:

```bash
poetry run python alma_digital_upload.py --config config.json --match-strategy marc-907e
```

- Extracts MARC 907$e values from bibliographic records
- Uses values as folder paths relative to `files_root`
- Uploads all files found in matched folders

See [FOLDER_PATH_WORKFLOW.md](FOLDER_PATH_WORKFLOW.md) for detailed instructions.

#### MMS ID Filename Strategy (Filename-Based Workflow)

Matches files by looking for filenames that match MMS IDs:

```bash
poetry run python alma_digital_upload.py --config config.json --match-strategy mms-id-filename
```

- Scans input folder for files named `{mms_id}.pdf`
- Matches files to records in the Alma set
- Simple one-file-per-record workflow

See [FILENAME_WORKFLOW.md](FILENAME_WORKFLOW.md) for detailed instructions.

### Step-by-Step Execution

Run individual steps for testing:

```bash
# Step 1: Get set members only
poetry run python alma_digital_upload.py --config config.json --step 1

# Step 2: Match files only
poetry run python alma_digital_upload.py --config config.json --step 2

# Step 3: Create representations only
poetry run python alma_digital_upload.py --config config.json --step 3

# Step 4: Upload and link only
poetry run python alma_digital_upload.py --config config.json --step 4

# All steps (default)
poetry run python alma_digital_upload.py --config config.json --step all
```

## Workflow Steps

### Step 1: Get Set Members
- Connects to Alma API
- Retrieves MMS IDs from the specified set
- Validates set type (must be BIB_MMS)

### Step 2: Match Files
- Uses selected strategy to match local files
- Generates match results for each MMS ID
- Reports matched vs unmatched records

### Step 3: Create Representations
- Checks for existing representations
- Creates new representations where needed
- Records representation IDs for linking

### Step 4: Upload and Link
- Uploads files to AWS S3
- Links files to Alma representations
- Reports success/failure for each file

## Utility Tools

### MARC 907 Extraction

Extract MARC 907 field data from records:

```python
from utils import Marc907Extractor

config = {"processing": {"field": "907"}, "output_settings": {}}
extractor = Marc907Extractor(config)
results = extractor.extract_from_mms_ids(mms_ids, bibs_client)
extractor.write_tsv_output(results)
```

### Folder Matching

Match TSV values to folder names:

```python
from utils import FolderMatcher

matcher = FolderMatcher()
records = matcher.read_tsv_file("marc_mapping.tsv")
folders = matcher.list_folders("/path/to/folders")
results, unmatched = matcher.match_records_to_folders(records, folders)
```

### Folder Renaming

Rename folders based on MARC mappings:

```python
from utils import FolderRenamer

config = {
    "input": {"tsv_file": "mapping.tsv", "folder_path": "/path"},
    "processing": {},
    "output_settings": {},
    "dry_run": True
}
renamer = FolderRenamer(config)
# See documentation for full usage
```

### Resume Helper

Resume interrupted uploads:

```python
from utils import ResumeHelper

helper = ResumeHelper()
log_files = helper.find_log_files()
results = helper.extract_processed_mms_ids(log_files[0])
helper.create_resume_tsv(original_tsv, results.exclude_from_resume)
```

## Testing

```bash
# Run smoke test
poetry run python scripts/smoke_project.py

# Run pytest
poetry run pytest tests/ -v
```

## Project Structure

```
Alma-Digital-Upload/
├── alma_digital_upload.py      # Main unified entry point
├── strategies/
│   ├── __init__.py
│   ├── base.py                 # Abstract MatchStrategy class
│   ├── marc_907e_strategy.py   # Folder-path matching
│   └── mms_id_filename_strategy.py  # Filename-based matching
├── utils/
│   ├── __init__.py
│   ├── marc_extraction.py      # MARC field extraction
│   ├── folder_matching.py      # Folder matching utilities
│   ├── folder_renaming.py      # Folder renaming utilities
│   └── resume_helper.py        # Resume upload helper
├── config/
│   └── config.example.json     # Example configuration
├── scripts/
│   └── smoke_project.py        # Smoke test
├── tests/
│   ├── test_imports.py
│   └── test_strategies.py
├── docs/
│   ├── README.md
│   ├── FOLDER_PATH_WORKFLOW.md
│   └── FILENAME_WORKFLOW.md
├── pyproject.toml
└── .gitignore
```

## Troubleshooting

### Common Issues

1. **Set not found**: Verify set ID and API permissions
2. **No files matched**: Check file paths and naming conventions
3. **AWS upload failures**: Verify credentials and bucket permissions
4. **Representation creation fails**: Check library code and access rights

### WSL Path Format

When running on WSL, use Linux path format:

```
# Correct
/mnt/c/Users/name/Documents/files

# Incorrect
C:/Users/name/Documents/files
```

## License

MIT License

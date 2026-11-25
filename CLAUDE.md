# CLAUDE.md — Alma Digital Upload

A command-line tool, run by hand, that attaches local files to Alma bib records as digital
representations: it reads an Alma set, matches files to its records, creates representations,
uploads the files to the institution's S3 bucket and links them. One run = one JSON config.
No scheduler entry and no CI. `main` → masedet DevSandbox; `prod` → masedet Prod (owner only).

## Layout
- `alma_digital_upload.py`: the CLI and the four steps (1 set members, 2 match, 3 representations,
  4 upload + link). Uses `almaapitk` (`AlmaAPIClient`, `Admin`, `BibliographicRecords`) and boto3.
- `strategies/`: how files match records. `marc-907e`: the folder `files_root/<907$e>`, every file
  in it. `mms-id-filename`: files named `<mms_id>.<ext>` in `files_root`, filtered by
  `file_extensions`. Each gives the S3 key part (`get_s3_path_key`).
- `utils/`: helpers not called by the CLI (907 extraction to TSV, folder matching/renaming,
  `ResumeHelper`, which parses the older loader's `alma_loader_*.log` only).
- `config/config.example.json`: the only config in git. `historical_logs/`: configs of past runs.
- `tests/`, `scripts/smoke_project.py`: offline only (imports, strategy matching on temp folders).

## Commands
- Install: `poetry install` (Python 3.12). `poetry.lock` is committed, so masedet installs
  exactly the tested versions (almaapitk 0.7.0).
- Tests: `poetry run pytest -q` and `poetry run python scripts/smoke_project.py`.
- Dry run: `poetry run python alma_digital_upload.py -c <config.json>` (reads Alma, writes nothing;
  a dry step 4 still needs the AWS variables set, or it stops).
- Live: add `--live`. `--step N` runs steps 1..N and stops (`--step 4` = everything).
- Needs `ALMA_SB_API_KEY` / `ALMA_PROD_API_KEY`, and for step 4 `AWS_ACCESS_KEY`, `AWS_SECRET`,
  `ALMA_SB_BUCKET_NAME` / `ALMA_PROD_BUCKET_NAME`. In the tau sandbox only SANDBOX live
  runs work (Sandbox bucket, with `ALMA_PROD_API_KEY` unset); production runs happen on the owner's machine.

## Rules
Enforced by code:
- Nothing writes without `--live` (`main()`: the CLI overrides `options.dry_run`, so the config
  value has no effect). Dry run still reads Alma (set, bibs, representations).
- Step 3 reuses the first existing representation of any usage type; it creates a
  `PRESERVATION_MASTER` only when a record has none (`step3_create_representations`).
- Real configs stay out of git: `.gitignore` drops `*_config.json`, `*_prod.json`,
  `*_sandbox.json`, data files (`*.pdf`, `*.tsv`, `*.csv`, `*.xls*`), `logs/`, `output/`.
- Errors are logged as exception type + record id, never the exception text (commit `ae0f4ca`,
  issue #2); keep it that way in new code. boto3/botocore loggers are pinned to WARNING.

Known risks (agents must know; open issues #12 and #13, not fixed):
- `alma.environment` must be exactly `SANDBOX` or `PRODUCTION`. almaapitk upper-cases it, but the
  bucket choice in `_get_aws_credentials` does not: `sandbox` = sandbox Alma + PRODUCTION bucket.
- Re-running a live job uploads and links every matched file again; there is no resume or
  "already linked" check in the CLI.

Written only:
- A change to the run path gets one SANDBOX live run before `main` is promoted to `prod`.
- Changing what is written to Alma or S3, or how files match records, needs a test that would
  fail without the change.
- Any `almaapitk` pin change follows `docs/almaapitk-bump-protocol.md` (binding): one bump at a
  time, from this repo, with an audit in `docs/almaapitk-<version>-audit.md`.

## Where to look
- `docs/README.md`: config fields, usage, steps. `docs/FOLDER_PATH_WORKFLOW.md` and
  `docs/FILENAME_WORKFLOW.md`: the two workflows end to end.
- Alma digital representations, S3 upload and linking: load the `alma-digital` skill; core API
  questions (auth, errors, sets): `alma-api-expert`.
- `AGENTS.md`: the reviewer's brief (Codex reviews, Claude builds).

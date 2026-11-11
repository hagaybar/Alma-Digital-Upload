# almaapitk bump protocol (binding)

Every change to this repo's `almaapitk` pin follows this document, step by step. It is binding:
a bump that skips a step is not done. `CLAUDE.md` points here.

**Why it exists.** This repo is a production consumer of `almaapitk`. It runs on masedet, and
`main` and `prod` auto-deploy on a schedule with no second checkpoint. A bad bump on a
scheduled repo can fail silently in production. A bump is therefore a small, audited change
made inside this repo, never a one-line pin edit pushed from somewhere else.

## Ground rules

- **One repo at a time, from this repo.** The bump is done by a session working in this repo,
  using this repo's tests. Never as part of a batch across consumer repos, and never driven
  from the AlmaAPITK repo. An AlmaAPITK release ends by opening this repo's bump issue.
- **Trigger:** the issue "Bump almaapitk to `<version>` (pin → validate → main → prod)" in this
  repo. Bump to the current release directly; skip intermediate versions.
- **Agents never merge to `prod` or push to it.** The owner merges the PR into `main` and
  promotes `main` → `prod`.
- **No production writes during validation.** Live checks against PRODUCTION are read-only.
  Anything that writes to Alma, sends email or uploads files runs only against the SANDBOX,
  with `ALMA_PROD_API_KEY` unset, or not at all.
- **No real identifiers** (user IDs, MMS IDs, report paths with names, emails) in commits, PRs,
  issues or the audit. Use placeholders.

## Steps

### 1. Branch

`git checkout main && git pull && git checkout -b chore/almaapitk-<version>`.

### 2. Audit first: `docs/almaapitk-<version>-audit.md`

Write it before changing the pin. Earlier audits in `docs/` are the model. Sections:

- **(a) Usage surface.** Every `almaapitk` import and call in this repo, with `file:line`:
  classes, methods and arguments, exceptions caught, environment variables relied on.
- **(b) Upstream changes** from the version in `poetry.lock` to `<version>`: the AlmaAPITK
  `CHANGELOG.md` sections in between, plus anything the bump issue calls out. When the
  CHANGELOG is unclear, diff the wheels (`pip download almaapitk==<v> --no-deps` for both,
  then `diff -ru`).
- **(c) Per-symbol classification** of the usage surface: unchanged / changed but compatible
  / breaking. Every breaking entry names its fix.
- **(d) Repo-specific risks** listed under "This repo" below.
- **(e) Verdict:** SAFE, SAFE WITH CHANGES (list them), or BLOCKED (stop and report upstream).

### 3. Pin and lock

- Set `almaapitk = ">=<version>"`, keeping the pin's style and its comment.
- Run `poetry update almaapitk`, never a bare `poetry lock` that refreshes everything. Then run
  `poetry check --lock` and confirm `poetry.lock` resolves `almaapitk <version>`.
- No other package may move. If one must, list it in the audit with the reason.
- In the TAU sandbox, prefix Poetry commands with `POETRY_VIRTUALENVS_IN_PROJECT=false`. A
  `.venv` in the folder belongs to the WSL host.

### 4. Code changes (only if the audit requires them)

Keep them minimal, and give each one a test that fails without it. Don't add unrelated work
to the bump branch.

### 5. L2: this repo's tests

Run the commands under "This repo". All green, with no new skips.

### 6. L3: live check

Run the commands under "This repo" and record what ran and the result in the audit. If a
required L3 step can't run where you are (for example missing config), the PR says so and the
owner runs it before merging.

### 7. Pull request into `main`

- Commit the audit, pin, lock and any code changes:
  `chore(deps): bump almaapitk to >=<version> (#<issue>)`.
- The PR body links the audit and gives the L2 result and the L3 result (or what is left for
  the owner). It says `Refs #<issue>`, not `Closes`.
- Tick the issue's boxes that are actually done.
- The owner merges.

### 8. DevSandbox (masedet)

After the next scheduled auto-deploy of `main`, confirm at least one clean cycle on the
DevSandbox clone (see "This repo" for what clean means). Note it in the issue.

### 9. Prod

The owner merges `main` → `prod`; the next auto-deploy pulls the new version into Prod. After
the next Prod run, check its logs and output, then close the issue.

### Rollback

Pin `almaapitk==<previous>` on `prod`, run `poetry update almaapitk`, and push. The next
auto-deploy restores the previous version. Report the regression in AlmaAPITK with a failing
test (AlmaAPITK rule R10).

## This repo: Alma-Digital-Upload

This is a **manually run, writing** job. It creates digital representations in Alma and links
S3 files to them. It is run on demand, not on a schedule, so the person running it is the last
safety net.

- **`poetry.lock` is gitignored here.** Step 3 still runs `poetry update almaapitk` and
  `poetry check --lock` locally to prove the pin resolves, but only `pyproject.toml` is
  committed. On masedet the version is whatever the pin resolves to at deploy time, so the pin
  floor is the real control. For a rollback, pin `==<previous>` rather than relying on a lock.
- **Usage surface:**
  - `alma_digital_upload.py`: `AlmaAPIClient(env)`, `Admin.get_set_info`,
    `Admin.get_bib_set_members`, `BibliographicRecords.get_representations`,
    `create_representation` and `link_file_to_representation`
  - `strategies/marc_907e_strategy.py` and `utils/marc_extraction.py`:
    `BibliographicRecords.get_marc_subfield`

  Check it with `grep -rn "almaapitk\|self\.bibs\.\|self\.admin\.\|get_marc_subfield" --include=*.py .`.
- **Repo-specific risks for (d):**
  - Anything that could create a representation twice: retries on POST, and the "existing
    representation" check.
  - Set member reading: completeness and paging.
  - `get_marc_subfield` semantics: `[]` for absent versus an error for a failed fetch.
  - The S3 link payload shape.
- **L2:** `poetry run pytest -q` and `poetry run python scripts/smoke_project.py`.
- **L3 (SANDBOX only, `ALMA_PROD_API_KEY` unset, Sandbox S3 bucket):** upload one test file
  end to end into a test set with one record.
- **Clean DevSandbox cycle:** not applicable (the job isn't scheduled). The next manual run
  after the merge is checked against its log before any production run.

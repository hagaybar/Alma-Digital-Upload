# Process: almaapitk-upgrade-verify

Verification-gated upgrade of the `almaapitk` dependency from `>=0.3.1` to `>=0.4.5`
on the `main` branch (Issue #1). The jump crosses a full minor (0.3.x → 0.4.x), so
the emphasis is proving the repo's used API surface still aligns under the new
version **before** anything is committed.

## Inputs
- `oldPin`: `almaapitk = ">=0.3.1"`
- `newPin`: `almaapitk = ">=0.4.5"`
- `targetVersion`: `0.4.5`
- `maxAttempts`: `3`
- `issue`: `1`

## Phases

1. **Inventory usage** (agent) — catalogue every almaapitk symbol/method/argument
   the repo depends on (`AlmaAPIClient`, `Admin`, `BibliographicRecords`,
   `.test_connection()`, etc.) with file:line locations. Read-only.
2. **Apply upgrade** (shell) — bump the pin in `pyproject.toml`, `poetry lock` to
   re-resolve, `poetry install`, capture the locked version.
3. **Verification convergence loop** (≤ `maxAttempts`), running in parallel:
   - **Tests** — `poetry run pytest -q`.
   - **API surface** (agent) — introspect the *installed* 0.4.5 package and confirm
     every inventoried symbol/signature still resolves and is compatible.
   - **Redaction** (shell) — offline check: exercise the toolkit's `TextFormatter`
     at DEBUG with a **fake** Authorization header and assert it is redacted. No
     network, no real keys.
   - **Compatibility review** (agent) — aggregate the three into a verdict +
     `artifacts/compatibility-report.md`. If misaligned, **remediate** (agent,
     minimal fix) and re-verify.
4. **Owner review before commit** (breakpoint) — show the diff +
   compatibility report; you approve / request changes / skip commit. Commit to
   `main` happens **only** on explicit approval.

## Guardrails
- Offline verification only — no live Alma API calls, no credentials touched.
- Work stays on `main`; no feature branch.
- Non-convergence after `maxAttempts` escalates to an owner breakpoint
  (abandon+revert, or override), never a silent commit.
- Commit is gated behind explicit owner approval.

## Outputs
`{ success, lockedVersion, aligned, committed, attempts }`

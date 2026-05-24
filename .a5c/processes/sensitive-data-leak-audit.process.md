# Process: sensitive-data-leak-audit (Issue #2)

Audit-first security review: find code paths that could write sensitive data
(API keys, tokens, Authorization headers, credential-bearing URLs, PII) to
stdout/stderr, logs, or output files. **No code changes during the audit** —
findings report first, fixes only after owner review (per the issue).

All work follows strict secret-handling: detect call sites and patterns, never
print secret values or dump credential files.

## Inputs
- `issue`: 2 · `projectRoot`: repo root · `maxAttempts`: 3

## Phases

1. **Audit (parallel, read-only)** — three agents cover the issue's checklist:
   - **Output paths** (§1, 2, 6): every print/log/pprint/stdout/stderr/traceback/
     warnings call + `__repr__`/`__str__`; classify DIRECT vs INDIRECT leak risk.
   - **File writes** (§3): open/write/to_csv/to_json/dump/dumps/FileHandler — what's
     written and whether it could carry secrets/PII.
   - **Config / log-config / history** (§4, 5, 7): checked-in secrets in
     config/fixtures/docs (presence only, no values), log level/handlers/filters
     (incl. verifying the almaapitk 0.4.5 redaction holds), lightweight `git log -S`
     history scan (history rewrite = out of scope, record only).
2. **Consolidate** — dedupe + severity-rank into `artifacts/findings-report.md`
   (path, line, category, why-sensitive, severity, proposed fix, patterns).
3. **Owner review breakpoint** — Apply all / Apply selected / Defer all / Abort.
4. **Apply fixes (conditional) + verify loop** (≤ `maxAttempts`): apply only
   approved fixes (minimal: remove / redact / log-safe-fields / guard DEBUG) →
   `pytest` + re-verify each fixed path → verdict; non-convergence escalates.
   Then **commit breakpoint** → commit to main (tracked files only).
5. **Summary comment breakpoint** → post the acceptance-criteria summary
   (findings / fixed / deferred / patterns) to issue #2 via `gh`.

## Guardrails
- Audit is strictly read-only; no edits before the review breakpoint.
- Strict secret-handling throughout (no values to output, no credential-file dumps).
- Stays on `main`. Commit and the GitHub comment are each owner-gated.
- Clean result (0 findings) is a valid outcome → skip fixes/commit, still post summary.

## Outputs
`{ success, totalFindings, fixed, deferred, committed, commented }`

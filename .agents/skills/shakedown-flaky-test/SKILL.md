---
name: shakedown-flaky-test
description: Shake down a flaky test (or subset of flaky tests) in dask/distributed. Reproduce locally with pytest-repeat, then either fix-and-verify locally or drive the `torture-test` GitHub Action via `gh` until CI is green. Use when the user says things like "debug flaky test <test name>", "fix intermittent failures in test <test name>", "torture test <test name>", "track down a flaky test", "reproduce a flaky failure", "fix intermittent test failures", or otherwise asks to shake out / harden a test that fails only sometimes.
---

# Shakedown a flaky test

Goal: turn a flaky test (or a subset of flaky tests) green, deterministically, without ever opening a PR or touching `main`.

## Hard rules — never violate

- **NEVER open a pull request on your own.**
- **NEVER commit to or push `main`.** All work happens on a new branch.
- Before opening or updating any PR, ask the user to explicitly confirm they fully reviewed, understood, and approved everything. (Maintainers treat this as non-negotiable.)

## Inputs you need from the user

1. The failing test selector(s) — e.g. `distributed/tests/test_client.py::test_client_submit`, a file, a `-k` pattern, or a marker. If the user only gave a test name, expand it to a runnable pytest node id.
2. **Examples of failed CI runs** (URLs or run IDs). If the user did not provide any, ask for them before doing anything else — the failure signature is what tells you whether a local repro is even plausibly the same bug. Accept:
   - GitHub Actions run URLs (`https://github.com/<owner>/<repo>/actions/runs/<run-id>[/job/<job-id>]`)
   - bare run IDs
   - pasted log snippets
   Pull the logs with `gh run view <run-id> --log [--job <job-id>]`. Do not fall back to `web_fetch` for github.com. If `gh` does not work, ensure that `gh auth status` returns all green and if not stop suggest to the user how to fix the issue.

## Step 1 — Confirm the test is not deterministic

Run the test once, locally, the normal way:

```bash
pixi run test --runslow <test-selector>
```

If it fails deterministically, **stop** — this is not a flaky test. Tell the user it fails every time and fix it as a normal bug.
If it passes, keep going.

Note down the wall-clock runtime of the run (parse it from the printed durations); you need it for the next step.

## Step 2 — Calibrate `--count` for a ~5 minute local run

`pytest-repeat` is already a project dependency (`pixi.toml`). Drive it with `--count N`:

```bash
pixi run test --runslow <test-selector> --count <N>
```

Compute `N` from the single-run runtime you noted in Step 1 so the total finishes in **~5 minutes** (300s):

```
N = max(100, round(300 / single_run_seconds))
```

Rules:
- **For a single test node id, `N` must be at least 100** even if one run is slow (e.g. 10s → still `--count 100`, accept the long runtime).
- For a *subset* of tests (file / `-k` / marker), the 100-minimum applies to the **whole selector**, not per test — but still aim for ~5 min total. If 100 runs of the whole subset would blow way past 5 min, reduce `N` so the batch fits ~5 min and say so out loud.

Sanity-check your math by running once with a small `--count` (e.g. 5) and extrapolating if Step 1's timing was noisy.

## Step 3 — Try to reproduce locally

Run the calibrated batch:

```bash
pixi run test <test-selector> --count <N>
```

- **If it fails at least once:** local reproduction achieved. Go to **Step 4**.
- **If it passes (all `N` green):** no local reproduction. Go to **Step 5**.

### Step 4 — Local reproduction: investigate, fix, verify

1. **Investigate.** Read the failure from the batch output (and the CI logs the user gave you). Form a hypothesis. Investigate the relevant source code under `distributed/`. Look at `utils_test.py` fixtures, timing-sensitive `gen_cluster` quirks, etc.
2. **Produce a fix.** Make the smallest correct change. Follow repo style: Black, 120 cols, Ruff rules B/TID/I/UP/RUF.
3. **Re-verify with 5 batch rounds.** Run the calibrated `--count` batch **5 separate times** (fresh `pixi run` process each time, so state resets between rounds), or stop early the moment a round fails:

   ```bash
   for i in 1 2 3 4 5; do
     echo "=== round $i ==="
     pixi run test <test-selector> --count <N> || { echo "FAIL round $i"; break; }
   done
   ```

   - If a round fails: the fix is wrong or incomplete. Go back to 4.1, revise, re-run the 5 rounds.
   - If all 5 rounds are green: done. Report to the user what you changed and why, ask for explicit review approval before any PR. END.

### Step 5 — No local reproduction: drive CI torture

You cannot reproduce locally, so reproduce in CI via the `torture-test` workflow.

#### 5a. Confirm the `torture-test` workflow exists

```bash
gh workflow list --all | grep -i torture
```

If `torture-test` is not present, **stop** and tell the user. Do not create it yourself unless explicitly asked.

#### 5b. Calibrate `--count` for ~30 minutes of CI

CI is slower and noisier than local. Compute `N` for a **~30 minute** CI run:

```
N = max(100, round(1800 / estimated_single_run_seconds))
```

Use the Step 1 single-run runtime as the estimate, then inflate by a CI overhead factor (CI is typically ~1.5–3x slower for distributed's `gen_cluster` tests; pick a factor from the first torture run's actual timing and refine). For a single test, **`N` must be at least 100**.

#### 5c. First torture run — confirm the failure reproduces in CI

Trigger the workflow on the current branch via `workflow_dispatch`:

```bash
gh workflow run torture-test \
  --ref <current-branch> \
  -f test=<test-selector> \
  -f count=<N>
```

(Adjust `-f` input names to match the workflow's actual `inputs:` definitions — read the workflow file first with `gh workflow view torture-test --yaml` or `read` on `.github/workflows/<file>`.)

Wait for it to finish:

```bash
gh run watch <run-id>      # or poll: gh run list --workflow torture-test --limit 1
```

Then pull the logs and **verify the failure happens at least once**:

```bash
gh run view <run-id> --log | grep -E "FAILED|Error|assert"
```

- If the failure **does not** reproduce in CI either: tell the user. Ask whether to bump `N` / widen the selector / try a different environment. Do not loop forever on a non-reproducing failure.
- If it **does** reproduce: continue.

#### 5d. Get off `main`

If the current branch is `main` (check with `git branch --show-current`), create and switch to a new branch **before writing anything**:

```bash
git switch -c torture-<short-test-name>
```

Never commit to `main`.

#### 5e. Write a tentative fix, commit, push

- Write the smallest plausible fix (you're flying blind without a local repro, so lean on the CI failure signature).
- Commit with a clear message.
- Push to the user's fork (the `origin` remote must point at the fork; verify with `git remote -v`):

  ```bash
  git push -u origin HEAD
  ```

  If `origin` points at `dask/distributed` (upstream) and the user has no fork remote, **stop** and ask the user to add their fork as a remote / tell you the fork's URL. Never force-push to upstream.

#### 5f. Trigger `torture-test` on the fork

```bash
gh workflow run torture-test \
  --ref <current-branch> \
  -f test=<test-selector> \
  -f count=<N> \
  --repo <user-fork-owner>/<repo>
```

(Use `--repo` to target the fork if the workflow lives there; if the workflow only exists upstream, trigger upstream's `workflow_dispatch` with `--ref <current-branch>` — but only if the branch is pushed to upstream, which it should not be for a fork. Prefer running the workflow from the fork. Confirm with the user which repo owns the workflow if unclear.)

Wait for CI to finish (`gh run watch` / `gh run list`), then check the result:

```bash
gh run view <run-id> --log | grep -E "FAILED|Error|assert|passed|failed"
```

#### 5g. Loop until green

Repeat **indefinitely**:

```
fix → commit → push → trigger torture-test on fork → wait for CI → check result
```

- If CI is **all green**: report the fix and a link to the final green run to the user. Ask for explicit review approval before any PR. END.
- If CI **still fails**: read the new failure, revise the fix, re-commit, re-push, re-trigger. Keep iterating. Each iteration is a new commit on the same branch.
- At the end of every CI run, re-check whether the failure has changed character; if it has, re-read the logs from scratch rather than fixating on the original hypothesis.

## Stopping conditions

- Test fails deterministically (Step 1) → handle as normal bug.
- `torture-test` workflow missing (Step 5a) → stop, ask user.
- No fork remote and no way to push the branch (Step 5e) → stop, ask user.
- Failure reproduces neither locally nor in CI across a couple of bumped-`N` attempts (Step 5c) → stop, report, ask user how to proceed.
- All verification rounds green (Step 4 or Step 5g) → report, END.

## Notes

- Local test command: `pixi run test <selector>`. CI-equivalent (with coverage, leaks, slow tests): `pixi run test-ci <selector>` — use this if you want to match CI behaviour more closely locally.
- `--count` comes from `pytest-repeat` (already in `pixi.toml`). Do not add the dependency.
- CI timeout per test is 300s (signal-based on Linux). Keep individual runs well under that or pytest-timeout will kill them and look like a flake.
- Resource-leak detection (`--leaks=fds,processes,threads`) is on in `test-ci`; leaks can masquerade as flakiness — keep them in mind when reading failures.
- Always read the workflow file before dispatching so you use the correct input names and `--ref` semantics.

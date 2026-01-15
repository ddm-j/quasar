---
description: "Comprehensive PR review with intelligent routing to specialized Quasar reviewers"
argument-hint: "[pr-number|branch|aspects]"
allowed-tools: ["Bash", "Glob", "Grep", "Read", "Task"]
---

# Quasar PR Review System

Review code changes using specialized agents tailored to the Quasar codebase. Changes are intelligently routed to domain experts.

**Arguments:** "$ARGUMENTS"

## Routing Rules

Map changed files to reviewers:

| Pattern | Reviewer | Model |
|---------|----------|-------|
| `db/schema/*.sql` | code-reviewer-db | sonnet |
| `**/database_handler.py` | code-reviewer-db | sonnet |
| `quasar/services/**/*.py` | code-reviewer-backend | sonnet |
| `quasar/lib/**/*.py` | code-reviewer-backend | sonnet |
| `web/src/**/*.js` | code-reviewer-frontend | sonnet |
| `web/src/**/*.jsx` | code-reviewer-frontend | sonnet |
| `tests/**/*.py` | code-reviewer-tests | haiku |
| *all changes* | code-reviewer-integration | opus |

## Workflow

### Phase 1: Analyze Changes

1. Determine review target:
   - If argument is a PR number: `gh pr diff <number>`
   - If argument is a branch: `git diff main...<branch>`
   - If no argument: `git diff` (unstaged) or `git diff --cached` (staged)

2. Get list of changed files:
   ```bash
   git diff --name-only [target]
   ```

3. Categorize files by domain:
   - **db**: `db/schema/*.sql`, `**/database_handler.py`
   - **backend**: `quasar/services/**/*.py`, `quasar/lib/**/*.py` (excluding tests)
   - **frontend**: `web/src/**/*.{js,jsx}`
   - **tests**: `tests/**/*.py`

4. Determine which reviewers to invoke based on changed files

### Phase 2: Semantic Chunking

For large diffs or files with mixed concerns:

1. If a single file has changes spanning multiple domains (rare in Quasar):
   - Split diff by function/class boundaries
   - Route chunks to appropriate reviewers

2. For `core.py` files (1000+ lines):
   - Identify changed functions/methods
   - Group by concern (routes vs business logic vs data access)
   - Provide focused context to each reviewer

### Phase 3: Launch Reviewers

**Parallel execution** (default):
- Launch all applicable domain reviewers simultaneously
- Each gets: relevant diff chunks + supporting context files

**Context to provide each reviewer**:
- **code-reviewer-db**: Diff + full schema files for reference
- **code-reviewer-backend**: Diff + related Pydantic schemas + base classes
- **code-reviewer-frontend**: Diff + API client files + column configs
- **code-reviewer-tests**: Diff + conftest.py + source files being tested

**Integration reviewer** runs last:
- Receives full diff + all other reviewer outputs
- Checks cross-layer consistency

### Phase 4: Synthesize Results

Combine all reviewer outputs into unified report:

```markdown
# PR Review Summary

**Target**: [PR #N | branch-name | working changes]
**Files Changed**: N
**Reviewers Invoked**: [list]

## Critical Issues (BLOCKER)
Must fix before merge.

| # | Issue | Reviewer | Location | Risk |
|---|-------|----------|----------|------|
| 1 | [description] | [reviewer] | `file:line` | [type] |

## Warnings
Should address.

| # | Issue | Reviewer | Location |
|---|-------|----------|----------|
| 1 | [description] | [reviewer] | `file:line` |

## Suggestions
Nice to have.

| # | Suggestion | Reviewer | Location |
|---|------------|----------|----------|
| 1 | [description] | [reviewer] | `file:line` |

## Coverage Gaps
Missing tests or incomplete implementation.

| # | Gap | Expected Location |
|---|-----|-------------------|
| 1 | [what's missing] | [where] |

## Security Notes
- [any security observations across reviewers]

## Integration Status
- API Contracts: [aligned|mismatched]
- Dead Code: [none|found (N items)]
- Completeness: [ready|needs-work]

## What Looks Good
- [positive observations]

## Recommended Actions
1. [first priority fix]
2. [second priority]
3. [etc.]
```

## Usage Examples

**Review current unstaged changes:**
```
/review-pr
```

**Review a specific PR:**
```
/review-pr 42
```

**Review a branch:**
```
/review-pr feat/new-feature
```

**Review only specific aspects:**
```
/review-pr db backend
# Only runs code-reviewer-db and code-reviewer-backend
```

**Quick review (skip integration):**
```
/review-pr quick
# Runs domain reviewers but skips code-reviewer-integration
```

## Agent Descriptions

| Agent | Focus | Model | When to Use |
|-------|-------|-------|-------------|
| code-reviewer-db | PostgreSQL, TimescaleDB, asyncpg, triggers | sonnet | Schema or query changes |
| code-reviewer-backend | FastAPI, Pydantic, async, APScheduler | sonnet | Service or library changes |
| code-reviewer-frontend | React 18, CoreUI Pro, state, API clients | sonnet | UI changes |
| code-reviewer-tests | pytest, fixtures, mocks, coverage | haiku | Test changes or coverage gaps |
| code-reviewer-integration | Cross-layer consistency, dead code | opus | Always (final pass) |

## Tips

- **Run early**: Before creating PR, not after
- **Address blockers first**: Critical issues block merge
- **Re-run after fixes**: Verify issues resolved
- **Use targeted reviews**: Specify aspects when you know the concern
- **Integration last**: It synthesizes findings from other reviewers

## De-conflicting Findings

When reviewers report overlapping issues:
1. Group by location (file:line)
2. Keep highest severity rating
3. Combine recommendations
4. Note which reviewers flagged it

## Notes

- Domain reviewers run in parallel (faster)
- Integration reviewer runs after domain reviewers complete
- Each reviewer uses project-specific patterns from CLAUDE.md
- Line numbers reference the changed files, not the diff

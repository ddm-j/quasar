# Git Workflow Guidelines

Quick reference for commit, branch, and merge best practices.

---

## Commits

### The Rule
One commit = one logical change. The codebase should be in a working state after each commit.

### Test Before Committing
- Does this commit do **one thing**?
- Is the code **working** (tests pass)?
- Can I describe it in one sentence **without using "and"**?

### Message Format
```
<type>(<scope>): <short description>

[optional body: explain WHY, not what]
```

**Types:** `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

**Examples:**
```
feat(datahub): add websocket support for live tick data
fix(registry): prevent duplicate strategy registrations
test(providers): add unit tests for kraken rate limiting
refactor(core): extract base provider class for reuse
```

---

## Branches

### Naming Convention
```
feat/short-description    # New features
fix/issue-or-description  # Bug fixes
refactor/what-changed     # Code restructuring
test/what-coverage        # Adding tests
docs/what-documented      # Documentation
```

### Workflow (GitHub Flow)
1. `main` is always deployable — never push broken code directly
2. Create feature branches for all work
3. Keep branches short-lived (merge within days, not weeks)
4. Use pull requests even when solo — they document decisions

```
main ─────●─────●─────●─────●─────●───────
           \         /     \     /
            feat/xxx       fix/yyy
```

---

## Quick Checklist

Before every commit:
- [ ] Single logical change?
- [ ] Tests pass?
- [ ] On the right branch?
- [ ] Message follows format?

Before merging to main:
- [ ] All tests pass?
- [ ] Code reviewed (even self-review)?
- [ ] Branch is up to date with main?


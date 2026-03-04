# Agent Instructions

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create tasks for anything that needs follow-up
1. **Run quality gates** (if code changed) - Tests, linters, builds
1. **Update issue status** - Close finished work, update in-progress items
1. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
1. **Clean up** - Clear stashes, prune remote branches
1. **Verify** - All changes committed AND pushed
1. **Hand off** - Provide context for next session

**CRITICAL RULES:**

- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
- NEVER use `--no-verify` to skip pre-commit hooks - fix the issue instead

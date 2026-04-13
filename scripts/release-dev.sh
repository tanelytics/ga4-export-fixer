#!/bin/bash
set -e

# Dev prerelease script
# Usage: bash scripts/release-dev.sh
#   --dry-run    Run checks and tests but skip publish and integration tests

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
  DRY_RUN=true
  echo "[dry-run mode]"
  echo ""
fi

# --- Pre-flight checks ---

# Must be on main branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$BRANCH" != "main" ]]; then
  echo "Error: must be on main branch (currently on '$BRANCH')"
  exit 1
fi

# Working tree must be clean
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Error: working tree is not clean"
  git status --short
  exit 1
fi

# npm auth check
NPM_USER=$(npm whoami 2>&1) || {
  echo "Error: not logged in to npm (run 'npm login' first)"
  echo "       npm whoami returned: $NPM_USER"
  exit 1
}

echo "Pre-flight checks passed"

# --- Unit tests ---

echo ""
echo "Running unit tests..."
npm test

# --- Version bump ---

echo ""
CURRENT_VERSION=$(node -p "require('./package.json').version")
echo "Current version: $CURRENT_VERSION"

npm version prerelease --preid dev --no-git-tag-version
NEW_VERSION=$(node -p "require('./package.json').version")
echo "New version: $NEW_VERSION"

# --- README generation ---

echo ""
echo "Generating README..."
npm run readme

# --- Commit + tag ---

git add package.json package-lock.json README.md
git commit -m "$NEW_VERSION"
git tag "v$NEW_VERSION"

if [[ "$DRY_RUN" == true ]]; then
  echo ""
  echo "[dry-run] Skipping publish and integration tests"
  echo "[dry-run] To undo: git tag -d v$NEW_VERSION && git reset --soft HEAD~1"
  exit 0
fi

# --- Publish ---

echo ""
read -p "Publish $NEW_VERSION to npm? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Aborted. To undo: git tag -d v$NEW_VERSION && git reset --soft HEAD~1"
  exit 1
fi

npm publish --tag dev

# --- Integration tests ---

echo ""
echo "Running integration tests..."
npm run test:integration

echo ""
echo "Done: $NEW_VERSION published and verified"

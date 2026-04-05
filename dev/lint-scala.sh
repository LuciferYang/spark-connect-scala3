#!/usr/bin/env bash
#
# lint-scala.sh — Clear scalafmt cache, format all Scala sources, then verify.
# Run this before every commit to ensure consistent formatting.
#
# Usage:
#   dev/lint-scala.sh          # format + check
#   dev/lint-scala.sh --check  # check only (CI mode, no auto-format)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "==> Clearing scalafmt cache..."
rm -rf .scalafmt.conf.cache

if [[ "${1:-}" == "--check" ]]; then
  echo "==> Running scalafmtCheckAll (check-only mode)..."
  build/sbt scalafmtCheckAll
else
  echo "==> Running scalafmtAll (format)..."
  build/sbt scalafmtAll
  echo "==> Running scalafmtCheckAll (verify)..."
  build/sbt scalafmtCheckAll
fi

echo "==> Scala formatting OK."

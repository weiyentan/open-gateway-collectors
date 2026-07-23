#!/bin/bash
# worktree-manager.sh - Manage isolated worktrees for issue implementation
set -euo pipefail

ACTION="${1:-}"
ISSUE_NUM="${2:-}"
SLUG="${3:-}"
REASON="${4:-}"

WORKTREES_DIR=".worktrees"
STATUS_DIR=".status"
WORKTREE_PATH="${WORKTREES_DIR}/issue-${ISSUE_NUM}-${SLUG}"
BRANCH="tmp/issue-${ISSUE_NUM}-${SLUG}"

case "$ACTION" in
  create)
    mkdir -p "${WORKTREES_DIR}"
    if [ -d "${WORKTREE_PATH}" ]; then
      echo "${WORKTREE_PATH}"
      exit 0
    fi
    git worktree add -b "${BRANCH}" "${WORKTREE_PATH}" master
    echo "${WORKTREE_PATH}"
    ;;
  assign)
    echo "${WORKTREE_PATH}"
    ;;
  clean)
    if [ -d "${WORKTREE_PATH}" ]; then
      git worktree remove "${WORKTREE_PATH}" 2>/dev/null || rm -rf "${WORKTREE_PATH}"
      git branch -D "${BRANCH}" 2>/dev/null || true
    fi
    echo "cleaned: ${WORKTREE_PATH}"
    ;;
  preserve)
    mkdir -p "${STATUS_DIR}"
    echo "Preserved ${WORKTREE_PATH} (${BRANCH}) reason: ${REASON}" >> "${STATUS_DIR}/preserved-worktrees.txt"
    echo "preserved: ${WORKTREE_PATH}"
    ;;
  list-preserved)
    cat "${STATUS_DIR}/preserved-worktrees.txt" 2>/dev/null || echo "none"
    ;;
  *)
    echo "Usage: $0 {create|assign|clean|preserve|list-preserved} <issue-num> <slug> [reason]"
    exit 1
    ;;
esac

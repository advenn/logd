#!/usr/bin/env bash
# Build the logd image for docker-compose.v1.yml from a git tag (default thesis-eval-1).
# It uses a temporary worktree, so the checkout you are working in is never touched.
set -euo pipefail
TAG="${1:-thesis-eval-1}"
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
wt="$(mktemp -d)"
trap 'git -C "$root" worktree remove --force "$wt" >/dev/null 2>&1 || true' EXIT
git -C "$root" worktree add -q --detach "$wt" "$TAG"
docker build -t "logd:$TAG" "$wt"
echo "built logd:$TAG from $(git -C "$root" rev-parse --short "$TAG^{commit}")"

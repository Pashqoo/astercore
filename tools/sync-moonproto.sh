#!/usr/bin/env bash
# Pull crates/moonproto up to the LATEST upstream moonproto (the trader's
# decision of 01.10: this core always tracks the newest rev, PLAN.md "Открытые
# решения" п. 7), or to the rev given as the first argument.
# The expected conflict is the `pub mod server;` hook in src/lib.rs — upstream
# edits that file too, so it is where to look first.
set -euo pipefail
cd "$(dirname "$0")/.."

UPSTREAM=https://github.com/Moonbot-Tech/MoonProtoBeta
# Upstream HEAD, not the terminal's Cargo.lock: a terminal checkout on this
# machine lags its own main (measured 01.10: local lock 6a6c419, terminal main
# 87c1725, upstream HEAD 9fd0490), and "latest" means upstream.
REV=${1:-$(git ls-remote "$UPSTREAM" HEAD | cut -f1)}
# A rev is passed straight to `subtree pull`, so it is checked for shape here:
# an empty or truncated value would otherwise reach git as a branch name.
[[ "$REV" =~ ^[0-9a-f]{40}$ ]] || { echo "not a 40-hex moonproto rev: '${REV}'" >&2; exit 1; }

CUR=$(cat MOONPROTO_REV)
if [[ "$CUR" == "$REV" ]]; then
  echo "already at $REV"; exit 0
fi

echo "syncing $CUR -> $REV"
# A conflicting pull aborts here under `set -e`, BEFORE MOONPROTO_REV moves, and
# leaves the tree mid-merge. That is recoverable but not obvious, so the way out
# is printed rather than remembered: upstream edits src/lib.rs too,
# so the hook is not the only place a conflict can land.
if ! git subtree pull --prefix crates/moonproto --squash "$UPSTREAM" "$REV" \
       -m "moonproto: sync to $REV"; then
  cat >&2 <<'MSG'
subtree pull failed or conflicted. MOONPROTO_REV is unchanged, so nothing is
pinned to a tree that was not merged. To recover:
  - abandon:  git merge --abort   (or: git reset --hard HEAD && git clean -fd crates/moonproto)
  - resolve:  fix the conflicts (src/lib.rs first — keep BOTH upstream's changes
              and the two hook lines), then: git add -A && git commit
              then re-run this script to pin the rev and re-check the vendor.
MSG
  exit 1
fi
# The pin is committed only once the vendor check passes: a rev that is not the upstream tree
# plus our hook must not be recorded as the one in use.
echo "$REV" > MOONPROTO_REV
tools/check-vendor.sh
git add MOONPROTO_REV && git commit -qm "moonproto: pin $REV"
echo "synced $CUR -> $REV. NEXT: leak-review of the delta BEFORE the first build or test —"
echo "  node ~/.claude/pipeline/leak-check.js diff --out <file>, then the leak-review agent."
echo "  Only then: cargo test --workspace"

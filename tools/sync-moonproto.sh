#!/usr/bin/env bash
# Pull crates/moonproto up to the moonproto rev pinned in the terminal's Cargo.lock.
# The expected conflict is the `pub mod server;` hook in src/lib.rs — and upstream
# edits src/lib.rs too (6a6c419 touched it), so that file is where to look first.
set -euo pipefail
cd "$(dirname "$0")/.."

UPSTREAM=https://github.com/Moonbot-Tech/MoonProtoBeta
# The sibling terminal checkout on this machine. Verified to exist rather than
# inherited: TInvestCore's copy defaulted to `../Moonterminal`, which is not
# what the checkout is called here, and awk under `set -e` then aborted the
# sync before it pulled anything.
LOCK=${1:-../moon-terminal/Cargo.lock}
[[ -f "$LOCK" ]] || { echo "terminal Cargo.lock not found: $LOCK" >&2; exit 1; }

# `f` is reset at each `[[package]]`: without it, a moonproto entry carrying no
# `source` line (a path dependency) made awk run on into the NEXT crate and
# print its rev — which the 40-hex guard below would have accepted as valid.
REV=$(awk '/^\[\[package\]\]/{f=0}
           /^name = "moonproto"$/{f=1}
           f&&/^source = /{sub(/.*#/,""); gsub(/"/,""); print; exit}' "$LOCK")
# A rev is passed straight to `subtree pull`, so it is checked for shape here:
# an empty or truncated match would otherwise reach git as a branch name.
[[ "$REV" =~ ^[0-9a-f]{40}$ ]] || { echo "no 40-hex moonproto rev in $LOCK (got '${REV}')" >&2; exit 1; }

CUR=$(cat MOONPROTO_REV)
if [[ "$CUR" == "$REV" ]]; then
  echo "already at $REV"; exit 0
fi

echo "syncing $CUR -> $REV (from $LOCK)"
# A conflicting pull aborts here under `set -e`, BEFORE MOONPROTO_REV moves, and
# leaves the tree mid-merge. That is recoverable but not obvious, so the way out
# is printed rather than remembered: upstream edits src/lib.rs too (6a6c419 did),
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
echo "$REV" > MOONPROTO_REV
git add MOONPROTO_REV && git commit -qm "moonproto: pin $REV"
tools/check-vendor.sh
echo "synced $CUR -> $REV; run: cargo test --workspace"

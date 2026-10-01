#!/usr/bin/env bash
# Verify crates/moonproto equals upstream at MOONPROTO_REV except for
# src/server/** and the `pub mod server;` hook in src/lib.rs.
#
# Read-only: it never writes inside crates/moonproto. An earlier version made
# src/lib.rs match upstream for the duration of one diff, which would have left
# the hook deleted had the script died in between.
set -euo pipefail
cd "$(dirname "$0")/.."

UPSTREAM=https://github.com/Moonbot-Tech/MoonProtoBeta
REV=$(cat MOONPROTO_REV)
OURS=crates/moonproto
# The two paths that are allowed to differ, and nothing else.
HOOK=src/lib.rs
MINE=src/server

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
UP="$TMP/up"

# Fetch failures are reported as their own exit code, not as a difference: a
# network error and a real mismatch used to look alike, and "vendored moonproto
# differs" is the wrong thing to say when upstream was never read.
if ! git clone -q --depth 1 --revision "$REV" "$UPSTREAM" "$UP" 2>"$TMP/clone.err"; then
  git init -q "$UP"
  if ! git -C "$UP" fetch -q --depth 1 "$UPSTREAM" "$REV" 2>"$TMP/fetch.err"; then
    echo "cannot read upstream $UPSTREAM at $REV:" >&2
    cat "$TMP/clone.err" "$TMP/fetch.err" >&2
    exit 2
  fi
  git -C "$UP" checkout -q FETCH_HEAD
fi
rm -rf "$UP/.git"

fail=0
report() { echo "vendored moonproto differs from upstream $REV ($1):" >&2; shift; printf '%s\n' "$@" >&2; fail=1; }

# 1. The file inventories must match, apart from the one directory that is ours.
#    Listed explicitly rather than with `diff -x server`, which excludes ANY path
#    whose basename is `server` at any depth — so a future upstream `tests/server`
#    would have gone unchecked forever.
list() ( cd "$1" && find . \( -type f -o -type l \) -print | grep -v "^\./$MINE/" | sort )
list "$UP"  > "$TMP/up.files"
list "$OURS" > "$TMP/our.files"
if ! inv=$(diff "$TMP/up.files" "$TMP/our.files"); then
  report "inventory" "$inv"
fi

# 2. Every upstream file compared byte for byte, except the hook, which is
#    compared against its own allow-list. Filtering the hook's two lines out of
#    a tree-wide diff is what used to let an upstream file that gained a line
#    reading `pub mod server;` pass unseen.
while IFS= read -r rel; do
  rel=${rel#./}
  [[ "$rel" == "$HOOK" ]] && continue
  [[ -e "$OURS/$rel" ]] || continue   # already named by the inventory check
  if ! d=$(diff -- "$UP/$rel" "$OURS/$rel"); then
    report "$rel" "$d"
  fi
done < "$TMP/up.files"

# 3. The hook: upstream's src/lib.rs plus exactly two added lines.
#    A tree with the hook REMOVED passes here, deliberately: it differs from
#    upstream by less, and this script's question is "was upstream edited". The
#    gate for "the hook is still there" is the build — `aster-core` fails on
#    `moonproto::server` without it.
hook=$(diff -- "$UP/$HOOK" "$OURS/$HOOK" \
       | grep -v -e '^[0-9,]*[acd][0-9,]*$' \
       | grep -v -e '^> // Astercore: ' -e '^> pub mod server;$' \
       || true)
[[ -z "$hook" ]] || report "$HOOK" "$hook"

# 4. Modes and symlinks, which `diff` cannot see: a vendored file turned
#    executable or into a symlink is a change to upstream code even though its
#    bytes match. `-perm` is spelled the POSIX way because BSD wants `+111` and
#    GNU wants `/111`, so neither is portable; and each `find` carries its own
#    status, since grouped in a pipeline the status was the last one's and a
#    probe that failed to run read as a pass.
probe() (
  cd "$1" || exit 2
  find . -path "./$MINE" -prune -o -type f \
       \( -perm -u+x -o -perm -g+x -o -perm -o+x \) -print || exit 2
  find . -path "./$MINE" -prune -o -type l -print || exit 2
)
probe "$UP"  | sort > "$TMP/up.modes"  || { echo "mode probe failed on upstream" >&2; exit 2; }
probe "$OURS" | sort > "$TMP/our.modes" || { echo "mode probe failed on the vendored tree" >&2; exit 2; }
if ! modes=$(diff "$TMP/up.modes" "$TMP/our.modes"); then
  report "modes/symlinks" "$modes"
fi

[[ $fail -eq 0 ]] || exit 1
echo "crates/moonproto == upstream $REV (+ $MINE, $HOOK hook)"

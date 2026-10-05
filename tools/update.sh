#!/bin/bash
# The core's update from its sources, asked for by the terminal (`TUpdateVersionCommand`).
#
# Run as root by aster-core-update.service, which aster-core-update.path starts when the core
# writes data/update-request. Installed as /usr/local/sbin/aster-core-update and never run from
# the clone it updates: what fetched code can change is the core, not its updater.
#
# The core's side and the file formats are in crates/aster-core/src/update.rs. In short:
#   data/update-request  the core  <id> <release 0|1> <target|-> <build> <commit>
#                                  (claimed by renaming it to update-running, removed at the end)
#   $STATE_DIR/update-state  this  <id> accepted | building <sha> <build> | staged <sha> <build> |
#                                  failed <why> | rolled-back <why> | done <sha> <build>
#   data/update-ack      the core  <id> swapped <sha> <from> | cancelled <why> | running <sha> <build>
#
# The core's user owns data/ and the core's directory, and the core holds the wallet key on an
# open UDP port: root therefore writes nothing there but by rename (which replaces a planted link
# instead of following it), keeps its own state in a root-owned directory, and never prints what
# it read from data/ before checking it.
#
# Only a commit on origin/main is built: the release is its head, a named build is a commit or
# a tag that main contains. A failed fetch, build or test never touches the running core. Once
# the core has swapped the binary in and restarted, the new one must say `running` within
# HEALTH_S, or the old binary is put back and the unit restarted.
set -euo pipefail

CORE_DIR=${ASTER_CORE_DIR:-/opt/aster-core}
UNIT=${ASTER_CORE_UNIT:-aster-core}
WORK=${ASTER_UPDATE_WORK:-/var/lib/aster-core-update}
REPO=${ASTER_UPDATE_REPO:-git@github.com:Pashqoo/astercore.git}
# Counted from the core's `swapped`: its stop (up to 40 s, aster-core.service), the unit's restart
# and the new core's start, which waits for the exchange's catalog.
HEALTH_S=${ASTER_UPDATE_HEALTH_S:-300}
# The core waits up to 10 min for a flat book before it swaps (update.rs, FLAT_WAIT_MS).
ACK_WAIT_S=${ASTER_UPDATE_ACK_WAIT_S:-660}

DATA=$CORE_DIR/data
EXE=$CORE_DIR/aster-core
SRC=$WORK/src
LOG=$WORK/build.log
STATE_DIR=${ASTER_UPDATE_STATE_DIR:-$WORK/state}
export CARGO_TARGET_DIR=$WORK/target
# No prompt can be answered here: a missing host key or key is a failed fetch, not a hang.
export GIT_SSH_COMMAND="ssh -o BatchMode=yes"

# Readable by the core, writable by root only.
mkdir -p -m 755 "$WORK" "$STATE_DIR"
exec 9>"$WORK/lock"
flock 9

[ -f "$DATA/update-request" ] || exit 0
# Claimed first: the path unit fires again on a request that is still there.
mv -f "$DATA/update-request" "$DATA/update-running"
trap 'rm -f "$DATA/update-running"' EXIT
read -r id release target from_build from_commit <"$DATA/update-running" || true

state() {
	printf '%s %s\n' "$id" "$*" >"$STATE_DIR/update-state.tmp"
	chmod 644 "$STATE_DIR/update-state.tmp"
	mv -f "$STATE_DIR/update-state.tmp" "$STATE_DIR/update-state"
	echo "update: $*"
}
fail() {
	state failed "$*"
	exit 1
}
# The word of the core's ack to this request, or nothing.
ack_kind() {
	local line ack_id kind _
	line=$(head -n1 "$DATA/update-ack" 2>/dev/null) || return 0
	read -r ack_id kind _ <<<"$line" || true
	if [ "$ack_id" = "$id" ]; then
		echo "$kind"
	fi
}

case $id in '' | *[!0-9]*)
	echo "update: a request without an id, ignored" >&2
	exit 1
	;;
esac
case $release in 0 | 1) ;; *) fail "bad request" ;; esac
if [ "$release" = 0 ] && ! [[ $target =~ ^[0-9A-Za-z_][0-9A-Za-z._/-]{0,63}$ && $target != *..* ]]; then
	fail "the name is not a commit or a tag"
fi
state accepted

if [ ! -d "$SRC/.git" ]; then
	timeout 300 git clone --quiet "$REPO" "$SRC" || fail "git clone of $REPO failed"
fi
timeout 300 git -C "$SRC" fetch --quiet --force --tags --prune origin || fail "git fetch failed"
main=$(git -C "$SRC" rev-parse origin/main)
if [ "$release" = 1 ]; then
	sha=$main
	target=main
else
	sha=$(git -C "$SRC" rev-parse --verify --quiet "$target^{commit}") || fail "no commit or tag $target"
fi
git -C "$SRC" merge-base --is-ancestor "$sha" "$main" || fail "$target is not on main"
build=$(git -C "$SRC" rev-list --count "$sha")
short=${sha:0:10}
if [ "$sha" = "$from_commit" ]; then
	fail "already on build $build ($short)"
fi
git -C "$SRC" checkout --quiet --force --detach "$sha"
state building "$sha" "$build"

(
	echo "=== $(date -u +%FT%TZ) build $build ($sha) for request $id"
	cd "$SRC"
	export ASTER_CORE_BUILD=$build ASTER_CORE_COMMIT=$sha
	started=$SECONDS
	timeout 1500 cargo build --release -p aster-core || exit 10
	echo "=== built in $((SECONDS - started)) s"
	timeout 600 cargo test --release -p aster-core --test loopback || exit 11
	echo "=== built and tested in $((SECONDS - started)) s"
) >>"$LOG" 2>&1 || case $? in
	10) fail "build $build failed — $LOG" ;;
	11) fail "build $build: the loopback test failed — $LOG" ;;
	*) fail "build $build failed — $LOG" ;;
esac
grep -h '^=== built' "$LOG" | tail -n1 | sed 's/^=== /update: /'

# Staged by rename from root's own directory (same filesystem): a link planted as .next is
# replaced, not written through.
install -m 755 "$CARGO_TARGET_DIR/release/aster-core" "$WORK/aster-core.next"
mv -fT "$WORK/aster-core.next" "$EXE.next"
state staged "$sha" "$build"

# The core swaps the binary in when it holds no position, or gives up after its own wait.
deadline=$((SECONDS + ACK_WAIT_S))
kind=
until [ "$kind" = swapped ] || [ "$kind" = cancelled ]; do
	if [ $SECONDS -ge $deadline ]; then
		rm -f "$EXE.next"
		fail "the core never took build $build"
	fi
	sleep 2
	kind=$(ack_kind)
done
if [ "$kind" = cancelled ]; then
	echo "update: the core cancelled build $build (its reason is in its journal)"
	exit 0
fi

# The new core says `running` once it serves; nothing in HEALTH_S and the old one goes back.
deadline=$((SECONDS + HEALTH_S))
until [ "$(ack_kind)" = running ]; do
	if [ $SECONDS -ge $deadline ]; then
		echo "update: build $build did not come up in $HEALTH_S s, rolling back" >&2
		# A unit that is inactive was stopped on purpose (exit 0: `systemctl stop`, the page,
		# the terminal): the old binary goes back, and the unit stays as the operator left it.
		was=$(systemctl is-active "$UNIT" 2>/dev/null || true)
		systemctl stop "$UNIT" || true
		if [ -f "$EXE.prev" ]; then
			mv -fT "$EXE.prev" "$EXE"
		fi
		state rolled-back "build $build did not come up in $HEALTH_S s"
		if [ "$was" != inactive ]; then
			systemctl reset-failed "$UNIT" 2>/dev/null || true
			systemctl start "$UNIT"
		fi
		exit 1
	fi
	sleep 2
done
state done "$sha" "$build"

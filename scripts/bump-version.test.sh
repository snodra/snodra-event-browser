#!/bin/sh
# Tests for bump-version.sh against copies of this repository's files: sh scripts/bump-version.test.sh
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

mkdir "$work/scripts"
cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/README.md" "$work/"
cp "$root/scripts/bump-version.sh" "$work/scripts/"
cd "$work" || exit 1

if sh scripts/bump-version.sh 9.8.7; then echo "ok   it runs"; else fail "it runs"; fi

pkg=$(awk '/^\[/ { p = ($0 == "[package]") } p && /^version = / { print; exit }' Cargo.toml)
[ "$pkg" = 'version = "9.8.7"' ] && echo "ok   Cargo.toml" || fail "Cargo.toml: $pkg"

lock=$(awk 'prev == "name = \"snodra-event-browser\"" { print; exit } { prev = $0 }' Cargo.lock)
[ "$lock" = 'version = "9.8.7"' ] && echo "ok   Cargo.lock" || fail "Cargo.lock: $lock"

grep -q 'snodra/snodra-event-browser:9\.8\.7' README.md && echo "ok   README.md" || fail "README.md"

# Only those lines change: one in each file. The libraries' entries in the lock keep theirs.
for f in Cargo.toml Cargo.lock README.md; do
  n=$(diff "$root/$f" "$f" | grep -c '^>')
  [ "$n" -eq 1 ] && echo "ok   one line in $f" || fail "$n lines changed in $f"
done

# A file without the expected line fails the bump.
for f in Cargo.toml Cargo.lock README.md; do cp "$f" "$f.good"; done
restore() { for f in Cargo.toml Cargo.lock README.md; do cp "$f.good" "$f"; done; }

printf '[package]\nname = "x"\n' >Cargo.toml
if sh scripts/bump-version.sh 9.8.8 2>/dev/null; then fail "a missing version fails"; else echo "ok   a missing version fails"; fi
restore

grep -v '^name = "snodra-event-browser"$' Cargo.lock.good >Cargo.lock
if sh scripts/bump-version.sh 9.8.8 2>/dev/null; then fail "a crate missing from the lock fails"; else echo "ok   a crate missing from the lock fails"; fi
restore

grep -v 'snodra/snodra-event-browser:' README.md.good >README.md
if sh scripts/bump-version.sh 9.8.8 2>/dev/null; then fail "a README without the image tag fails"; else echo "ok   a README without the image tag fails"; fi
restore

# Only N.N.N is a version; nothing is written for anything else.
for bad in 1.x 0.7 1..2 . 0.07.0 1.2.3.4 '' '1.2.3
4.5.6'; do
  if sh scripts/bump-version.sh "$bad" 2>/dev/null; then
    fail "the version '$bad' is refused"
  elif diff -q Cargo.toml.good Cargo.toml >/dev/null && diff -q Cargo.lock.good Cargo.lock >/dev/null \
    && diff -q README.md.good README.md >/dev/null; then
    echo "ok   the version '$bad' is refused"
  else
    fail "the version '$bad' changed a file"
  fi
done

exit $failed

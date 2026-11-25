#!/usr/bin/env bash
# Compile requirements.txt and requirements-dev.txt: exact versions, with hashes.
#   ./compile-requirements.sh                        keep the pinned versions, apply changes of the .in files
#   ./compile-requirements.sh --upgrade              move everything to the latest allowed versions
#   ./compile-requirements.sh --upgrade-package X    move one package
#   ./compile-requirements.sh --check                fail when the compiled files do not match the .in files
# Needs the uv version below (CI installs the same one).
set -euo pipefail
cd "$(dirname "$0")"

UV_VERSION="0.13.10"
found="$(uv --version | awk '{print $2}')"
if [ "$found" != "$UV_VERSION" ]; then
  echo "uv $UV_VERSION is required, found $found" >&2
  exit 2
fi

export UV_CUSTOM_COMPILE_COMMAND="./compile-requirements.sh"
OPTIONS=(--python-version 3.12 --python-platform x86_64-manylinux_2_28 --generate-hashes --quiet)

compile() {  # <directory of the output> [uv options]
  local out="$1"; shift
  uv pip compile requirements.in requirements-vcs.txt --no-emit-package ikerketa \
    "${OPTIONS[@]}" "$@" -o "$out/requirements.txt" >/dev/null
  uv pip compile requirements-dev.in -c "$out/requirements.txt" \
    "${OPTIONS[@]}" "$@" -o "$out/requirements-dev.txt" >/dev/null
}

if [ "${1:-}" = "--check" ]; then
  # Over a copy of the committed files: a new release on the index never fails this check.
  tmp="$(mktemp -d)"
  trap 'rm -r -- "$tmp"' EXIT
  cp requirements.txt requirements-dev.txt "$tmp/"
  compile "$tmp"
  versions() { grep -E '^[A-Za-z0-9][^ ]*==' "$1" | sed 's/ .*//'; }
  status=0
  for name in requirements.txt requirements-dev.txt; do
    if ! diff <(versions "$name") <(versions "$tmp/$name"); then
      echo "$name does not match its .in file: run ./compile-requirements.sh" >&2
      status=1
    fi
  done
  exit "$status"
fi

compile . "$@"

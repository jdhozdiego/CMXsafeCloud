#!/bin/sh
set -eu
export CMXSAFE_BUNDLE_BIN_ROOT="${CMXSAFE_BUNDLE_BIN_ROOT:-/opt/cmxsafe/bin}"
BUNDLE_ROOT="${CMXSAFE_BUNDLE_ROOT:-/bundle}"

if [ ! -d "$BUNDLE_ROOT" ]; then
  echo "CMXsafe endpoint image expects a mounted bundle directory at $BUNDLE_ROOT" >&2
  exit 1
fi

cd "$BUNDLE_ROOT"

if [ $# -eq 0 ]; then
  set -- ./run-forever
fi

exec "$@"

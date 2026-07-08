#!/usr/bin/env bash
# Rebuilds ariadne's SDK libs, re-packs them as tarballs into this repo's
# vendor/ariadne/, and reinstalls them here. Run whenever ariadne's SDK changes.
#
#   ./scripts/sync-ariadne-sdk.sh
#
# Assumes ariadne lives as a sibling of this repo. Override with ARIADNE_DIR.
set -euo pipefail

ECOMMERCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARIADNE_DIR="${ARIADNE_DIR:-$(cd "$ECOMMERCE_DIR/../ariadne" && pwd)}"
VENDOR_DIR="$ECOMMERCE_DIR/vendor/ariadne"
LIBS="protocol transport-core transport-kafka sdk-nestjs"

echo "ariadne:   $ARIADNE_DIR"
echo "ecommerce: $ECOMMERCE_DIR"

# Load Node 22 if nvm is present (ariadne requires Node 22).
if [ -s "$HOME/.nvm/nvm.sh" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.nvm/nvm.sh"
  nvm use 22 >/dev/null
fi

echo "==> building ariadne libs"
( cd "$ARIADNE_DIR" && npx nx run-many -t build --projects=protocol,transport-core,transport-kafka,sdk-nestjs )

echo "==> packing tarballs into $VENDOR_DIR"
mkdir -p "$VENDOR_DIR"
rm -f "$VENDOR_DIR"/*.tgz
for lib in $LIBS; do
  ( cd "$ARIADNE_DIR/dist/libs/$lib" && npm pack --pack-destination "$VENDOR_DIR" >/dev/null )
done
ls -1 "$VENDOR_DIR"

echo "==> reinstalling in ecommerce-kafka"
( cd "$ECOMMERCE_DIR" && npm install )

echo "==> done"

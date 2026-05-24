#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

version="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n1)"
out_dir="packaging/fedora"
vendor_dir="$out_dir/vendor"
archive="$out_dir/vendor-$version.tar.zst"

rm -rf "$vendor_dir"
cargo vendor --locked "$vendor_dir" >/dev/null
tar --zstd -cf "$archive" -C "$out_dir" vendor

printf 'Wrote %s\n' "$archive"

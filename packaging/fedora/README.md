COPR notes for `tur`

This spec expects a vendored Cargo source archive alongside the GitHub
release tarball.

Build sources:

1. Run `packaging/fedora/make-sources.sh`
2. Upload `vendor-<version>.tar.zst` to your COPR dist-git sources
3. Keep `Source0` pointed at the signed GitHub tag archive

Local build:

```bash
fedpkg --release rawhide srpm
mock -r fedora-rawhide-x86_64 --rebuild *.src.rpm
```

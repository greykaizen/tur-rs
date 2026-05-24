Name:           tur
Version:        0.9.2
Release:        1%{?dist}
Summary:        Hyper-fast concurrent download manager CLI

License:        GPL-3.0-only
URL:            https://github.com/greykaizen/tur-rs
Source0:        %{url}/archive/refs/tags/v%{version}/tur-rs-%{version}.tar.gz
Source1:        vendor-%{version}.tar.zst

BuildRequires:  cargo
BuildRequires:  gcc
BuildRequires:  rust

Requires:       glibc

%description
tur is a relentless, high-concurrency download manager written in Rust.
It provides a fast CLI/TUI interface backed by adaptive scheduling and
multi-connection downloads.

%prep
%autosetup -n tur-rs-%{version}
tar --zstd -xf %{SOURCE1}
mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
EOF

%build
cargo build --release --locked --frozen --offline --bin tur

%install
install -D -m 0755 target/release/tur %{buildroot}%{_bindir}/tur

%check
%{buildroot}%{_bindir}/tur --help >/dev/null

%files
%license LICENSE
%doc README.md
%{_bindir}/tur

%changelog
* Fri May 22 2026 Kaizen <kaizen@example.com> - 0.9.2-1
- Initial COPR package

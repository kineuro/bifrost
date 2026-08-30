#!/bin/sh
# Installs the bifrost command line tool on Linux or macOS.
#   curl -fsSL https://bifrost.kineuro.se/get | sh
# Puts the binary in ~/.local/bin (or /usr/local/bin when run as root). Re-run to update; or `bifrost update`.
set -e
BASE="${BIFROST_URL:-https://bifrost.kineuro.se}"
REL="https://github.com/kineuro/bifrost/releases/latest/download"
os=$(uname -s | tr '[:upper:]' '[:lower:]'); arch=$(uname -m)
case "$os" in linux|darwin) ;; *) echo "unsupported OS: $os (use the PowerShell installer on Windows)"; exit 1;; esac
case "$arch" in x86_64|amd64) arch=amd64;; arm64|aarch64) arch=arm64;; *) echo "unsupported architecture: $arch"; exit 1;; esac
file="bifrost-$os-$arch"
if [ "$(id -u)" = 0 ]; then dir=/usr/local/bin; else dir="$HOME/.local/bin"; fi
mkdir -p "$dir"
tmp=$(mktemp)
if curl -fsSL "$REL/$file" -o "$tmp" 2>/dev/null; then src="$REL"; echo "downloaded $file from the latest GitHub release"; else src="$BASE/dl"; echo "downloading $src/$file"; curl -fsSL "$src/$file" -o "$tmp"; fi
sum=$(curl -fsSL "$src/SHA256SUMS" | grep " $file\$" | awk '{print $1}')
if [ -n "$sum" ]; then
  have=$( (sha256sum "$tmp" 2>/dev/null || shasum -a 256 "$tmp") | awk '{print $1}')
  [ "$have" = "$sum" ] || { echo "checksum mismatch, aborting"; rm -f "$tmp"; exit 1; }
fi
chmod 755 "$tmp"; mv "$tmp" "$dir/bifrost"
[ "$os" = darwin ] && xattr -d com.apple.quarantine "$dir/bifrost" 2>/dev/null || true
echo "installed $dir/bifrost ($("$dir/bifrost" version 2>/dev/null || echo ok))"
case ":$PATH:" in *":$dir:"*) ;; *) echo "add this to your shell profile: export PATH=\"$dir:\$PATH\"";; esac
echo "next: bifrost login <token>   then   bifrost push <folder>   or   bifrost pull <folder>"

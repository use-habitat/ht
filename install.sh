#!/bin/sh
set -eu

VERSION="${HT_VERSION:-latest}"
HOME_DIR="${HT_HOME:-$HOME/.ht}"
RELEASE_BASE_URL="${HT_RELEASE_BASE_URL:-https://app.use-habitat.com/cli}"

case "$(uname -s)" in
  Darwin) OS=darwin ;;
  Linux) OS=linux ;;
  *) echo "HT currently supports macOS and Linux." >&2; exit 1 ;;
esac

case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64) ARCH=x64 ;;
  *) echo "Unsupported CPU architecture: $(uname -m)" >&2; exit 1 ;;
esac

ASSET="ht-${OS}-${ARCH}"
case "$RELEASE_BASE_URL" in
  https://*|http://*) ;;
  *) echo "HT_RELEASE_BASE_URL must be an http(s) URL." >&2; exit 1 ;;
esac
RELEASE_BASE_URL="${RELEASE_BASE_URL%/}"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/ht-install.XXXXXX")
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT HUP INT TERM

if [ "$VERSION" = "latest" ]; then
  curl --fail --location --silent --show-error "$RELEASE_BASE_URL/latest" --output "$TMP_DIR/version"
  VERSION=$(tr -d '\r\n' < "$TMP_DIR/version")
fi

case "$VERSION" in
  v[0-9][0-9A-Za-z.-]*) ;;
  *) echo "HT_VERSION must be a release version such as v0.4.0." >&2; exit 1 ;;
esac

BASE_URL="$RELEASE_BASE_URL/$VERSION"
TMP_BINARY="$TMP_DIR/$ASSET"
TMP_CHECKSUMS="$TMP_DIR/SHA256SUMS"
curl --fail --location --silent --show-error "$BASE_URL/$ASSET" --output "$TMP_BINARY"
curl --fail --location --silent --show-error "$BASE_URL/SHA256SUMS" --output "$TMP_CHECKSUMS"

EXPECTED=$(awk -v asset="$ASSET" '$2 == asset { print $1 }' "$TMP_CHECKSUMS")
if [ -z "$EXPECTED" ]; then
  echo "The release checksum does not contain $ASSET." >&2
  exit 1
fi
if command -v shasum >/dev/null 2>&1; then
  ACTUAL=$(shasum -a 256 "$TMP_BINARY" | awk '{ print $1 }')
elif command -v sha256sum >/dev/null 2>&1; then
  ACTUAL=$(sha256sum "$TMP_BINARY" | awk '{ print $1 }')
else
  echo "HT needs shasum or sha256sum to verify the download." >&2
  exit 1
fi
if [ "$EXPECTED" != "$ACTUAL" ]; then
  echo "HT download checksum verification failed." >&2
  exit 1
fi

mkdir -p "$HOME_DIR/bin" "$HOME_DIR/libexec"
mv "$TMP_BINARY" "$HOME_DIR/libexec/ht"
chmod 755 "$HOME_DIR/libexec/ht"
cat > "$HOME_DIR/bin/ht" <<EOF
#!/bin/sh
exec "$HOME_DIR/libexec/ht" "\$@"
EOF
chmod 755 "$HOME_DIR/bin/ht"

RESOLVED_HT=$(command -v ht 2>/dev/null || true)
PATH_LAUNCHER=""
if [ -z "$RESOLVED_HT" ]; then
  OLD_IFS=$IFS
  IFS=:
  for PATH_ENTRY in $PATH; do
    case "$PATH_ENTRY" in
      /*)
        if [ -d "$PATH_ENTRY" ] && [ -w "$PATH_ENTRY" ] && [ ! -e "$PATH_ENTRY/ht" ]; then
          PATH_LAUNCHER="$PATH_ENTRY/ht"
          break
        fi
        ;;
    esac
  done
  IFS=$OLD_IFS

  if [ -n "$PATH_LAUNCHER" ]; then
    ln -s "$HOME_DIR/bin/ht" "$PATH_LAUNCHER"
  fi
fi

PROFILE_UPDATED=false
if [ -z "$PATH_LAUNCHER" ] && [ -z "$RESOLVED_HT" ]; then
  case "${SHELL:-}" in
    */zsh) PROFILE="$HOME/.zshrc" ;;
    */bash) PROFILE="$HOME/.bashrc" ;;
    *) PROFILE="$HOME/.profile" ;;
  esac
  PATH_LINE="export PATH=\"$HOME_DIR/bin:\$PATH\""
  if [ -w "$PROFILE" ] || [ ! -e "$PROFILE" ] && [ -w "$(dirname "$PROFILE")" ]; then
    if ! grep -Fqx "$PATH_LINE" "$PROFILE" 2>/dev/null; then
      printf '\n# Habitat CLI\n%s\n' "$PATH_LINE" >> "$PROFILE"
    fi
    PROFILE_UPDATED=true
  fi
fi

echo "HT $VERSION installed at $HOME_DIR/bin/ht"
if [ -n "$PATH_LAUNCHER" ]; then
  echo "The ht command is ready in this Terminal."
elif [ -n "$RESOLVED_HT" ] && [ "$RESOLVED_HT" != "$HOME_DIR/bin/ht" ]; then
  echo "An existing ht command currently resolves first at $RESOLVED_HT."
  echo "Put $HOME_DIR/bin before it in PATH, or use the full path below."
elif [ "$PROFILE_UPDATED" = true ]; then
  echo "New terminals will have the ht command ready."
  echo "To continue in this Terminal, run: $HOME_DIR/bin/ht setup"
else
  echo "Run HT with: $HOME_DIR/bin/ht"
fi

echo "Next: ht setup"

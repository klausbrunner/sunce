#!/bin/sh

set -eu

OUT_DIR=$(find target -path '*/build/sunce-*/out' -type d 2>/dev/null | head -n 1)

if [ -z "${OUT_DIR}" ]; then
    printf 'Error: Completion files not found. Run '\''cargo build'\'' first.\n' >&2
    exit 1
fi

mkdir -p completions

cp "${OUT_DIR}/sunce.bash" completions/
cp "${OUT_DIR}/_sunce" completions/
cp "${OUT_DIR}/sunce.fish" completions/
cp "${OUT_DIR}/_sunce.ps1" completions/
cp "${OUT_DIR}/sunce.elv" completions/

echo "Completion files copied to ./completions/"
echo ""
echo "To install completions manually:"
echo ""
echo "Bash:"
echo "  sudo cp completions/sunce.bash /usr/local/etc/bash_completion.d/"
echo "  or: cp completions/sunce.bash ~/.local/share/bash-completion/completions/sunce"
echo ""
echo "Zsh:"
echo "  sudo cp completions/_sunce /usr/local/share/zsh/site-functions/"
echo "  or: cp completions/_sunce ~/.zsh/completions/"
echo ""
echo "Fish:"
echo "  cp completions/sunce.fish ~/.config/fish/completions/"
echo ""
echo "PowerShell:"
echo "  Copy completions/_sunce.ps1 to your PowerShell profile directory"
echo ""
echo "Elvish:"
echo "  cp completions/sunce.elv ~/.elvish/lib/"
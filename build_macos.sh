#!/bin/bash
set -e

echo "=== TG Forwarder macOS Build ==="

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt 2>/dev/null || pip3 install -r requirements.txt

# Clean previous build
rm -rf build dist

# Build
echo "Building app bundle..."
python3 -m PyInstaller tgforwarder.spec --clean

# Remove quarantine attribute
xattr -cr "dist/TG Forwarder.app"

# Install to /Applications
echo "Installing to /Applications..."
osascript -e 'tell application "TG Forwarder" to quit' 2>/dev/null || true
sleep 1
rm -rf "/Applications/TG Forwarder.app"
cp -R "dist/TG Forwarder.app" "/Applications/TG Forwarder.app"

echo ""
echo "=== Build Complete ==="
echo "Installed: /Applications/TG Forwarder.app"
echo "To run:    open '/Applications/TG Forwarder.app'"
echo "Data dir:  ~/.tgforwarder/"

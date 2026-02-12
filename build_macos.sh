#!/bin/bash
set -e

echo "=== TG Forwarder macOS Build ==="

# Install build dependency
pip install pyinstaller 2>/dev/null || pip3 install pyinstaller

# Clean previous build
rm -rf build dist

# Build
pyinstaller tgforwarder.spec --clean

echo ""
echo "=== Build Complete ==="
echo "App: dist/TG Forwarder.app"
echo ""
echo "To run: open 'dist/TG Forwarder.app'"
echo "Data directory: ~/.tgforwarder/"

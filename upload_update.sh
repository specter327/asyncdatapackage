#!/bin/bash

set -e

PROJECT_NAME="asyncdatapackage"
GITHUB_REPO="git@github.com:specter327/asyncdatapackage.git"

echo
echo "=================================="
echo "ASYNCDATAPACKAGE RELEASE TOOL"
echo "=================================="
echo

CURRENT_VERSION=$(grep '^version =' pyproject.toml | cut -d'"' -f2)

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

PATCH=$((PATCH + 1))

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"

echo "Current version : $CURRENT_VERSION"
echo "Next version    : $NEW_VERSION"
echo

sed -i "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml

echo "[+] Version updated"
echo

echo "[+] Cleaning previous artifacts"
rm -rf build
rm -rf dist
rm -rf *.egg-info

echo
echo "[+] Building package"
python3 -m build

echo
echo "[+] Validating package"
twine check dist/*

echo
echo "[+] Git operations"

git add .

if ! git diff --cached --quiet
then
    git commit -m "Release v${NEW_VERSION}"
fi

git tag -f "v${NEW_VERSION}"

git remote get-url origin >/dev/null 2>&1 || \
git remote add origin "$GITHUB_REPO"

git push origin main
git push origin "v${NEW_VERSION}" --force

echo
echo "[+] Uploading to PyPI"

twine upload dist/*

echo
echo "=================================="
echo "RELEASE COMPLETE"
echo "=================================="
echo
echo "Version : $NEW_VERSION"
echo "PyPI    : https://pypi.org/project/${PROJECT_NAME}/"
echo "GitHub  : https://github.com/specter327/asyncdatapackage"
echo

echo "=================================="
echo "UNINSTALLING PREVIOUS LIBRARY VERSION"
echo "=================================="
echo
CWD="$(pwd)";
cd ..;
pip uninstall asyncdatapackage --break-system-packages
sleep 10
pip install asyncdatapackage --break-system-packages
echo
cd $CWD;
echo "SCRIPT FINISHED"
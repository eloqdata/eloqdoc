#!/bin/bash
set -euxo pipefail

MAIN_REPO_PATH="."
SUBMODULE_PATH="./src/mongo/db/modules/eloq"

echo "Searching for modified/added C++/H files in main repo ($MAIN_REPO_PATH) and submodule ($SUBMODULE_PATH)..."

# Get files from the main repository, excluding the submodule directory itself
MAIN_FILES=$( ( \
    git diff --name-only --cached --diff-filter=d -- "$MAIN_REPO_PATH"; \
    git diff --name-only --diff-filter=d -- "$MAIN_REPO_PATH"; \
    git ls-files --others --exclude-standard -- "$MAIN_REPO_PATH" \
) | grep -v "^${SUBMODULE_PATH}/" || true ) # Exclude files *within* the submodule path

# Get files from within the submodule directory
# Use git -C to run commands inside the submodule path
# Prefix results with the submodule path
SUBMODULE_FILES=$( ( \
    git -C "$SUBMODULE_PATH" diff --name-only --cached --diff-filter=d; \
    git -C "$SUBMODULE_PATH" diff --name-only --diff-filter=d; \
    git -C "$SUBMODULE_PATH" ls-files --others --exclude-standard \
) | sed "s|^|$SUBMODULE_PATH/|" || true )

# Combine lists, filter for C++/H files, exclude pb.h/pb.cc (case-insensitive), and ensure uniqueness
FILES_TO_FORMAT=$( (echo "$MAIN_FILES"; echo "$SUBMODULE_FILES") | \
                   sort -u | \
                   grep -E '\.(cpp|h)$' | \
                   grep -ivE '\.pb\.(h|cc)$' || true) # Use -E, escape dots, keep -i

if [ -z "$FILES_TO_FORMAT" ]; then
    echo "No modified or added .cpp/.h files found in the specified locations."
    exit 0
fi

echo "Found files to format:"
echo "$FILES_TO_FORMAT"

# Format the files using clang-format-14
# Assumes .clang-format exists in the current directory
echo "$FILES_TO_FORMAT" | while IFS= read -r file; do
    # Check if file exists before formatting
    if [ -f "$file" ]; then
        echo "Formatting $file..."
        clang-format-14 -i "$file"
    else
        echo "Skipping formatting for non-existent file: $file"
    fi
done

echo "Formatting complete."

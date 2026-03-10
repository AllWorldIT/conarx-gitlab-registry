#!/bin/bash

# validate-migration-timestamps.sh - Validates that new migration files have timestamps
# newer than the latest migration in the target branch.
#
# This prevents migration ordering issues when parallel branches add migrations
# with different timestamps that can deploy out of chronological order.

set -euo pipefail

MIGRATIONS_DIR="registry/datastore/migrations"
MIGRATION_DIRS=("premigrations" "postmigrations")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Validates that new migration files have timestamps newer than the latest
migration in the target branch.

Options:
    -t, --target BRANCH    Target branch to compare against (default: origin/master)
    -b, --base SHA         Base commit SHA for comparison (default: merge-base with target)
    -h, --help             Show this help message

Environment variables (used in CI):
    CI_MERGE_REQUEST_TARGET_BRANCH_NAME    Target branch name
    CI_MERGE_REQUEST_DIFF_BASE_SHA         Base commit SHA for diff

Examples:
    # Local development (compares against origin/master)
    ./script/validate-migration-timestamps.sh

    # Compare against a specific branch
    ./script/validate-migration-timestamps.sh --target origin/main

    # CI mode (uses environment variables automatically)
    CI_MERGE_REQUEST_TARGET_BRANCH_NAME=master CI_MERGE_REQUEST_DIFF_BASE_SHA=abc123 ./script/validate-migration-timestamps.sh
EOF
}

TARGET_BRANCH=""
BASE_SHA=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)
            TARGET_BRANCH="$2"
            shift 2
            ;;
        -b|--base)
            BASE_SHA="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$TARGET_BRANCH" ]]; then
    if [[ -n "${CI_MERGE_REQUEST_TARGET_BRANCH_NAME:-}" ]]; then
        TARGET_BRANCH="origin/${CI_MERGE_REQUEST_TARGET_BRANCH_NAME}"
    else
        TARGET_BRANCH="origin/master"
    fi
fi

if [[ -z "$BASE_SHA" ]]; then
    if [[ -n "${CI_MERGE_REQUEST_DIFF_BASE_SHA:-}" ]]; then
        BASE_SHA="${CI_MERGE_REQUEST_DIFF_BASE_SHA}"
    else
        BASE_SHA=$(git merge-base HEAD "$TARGET_BRANCH" 2>/dev/null || echo "")
        if [[ -z "$BASE_SHA" ]]; then
            echo -e "${YELLOW}Warning: Could not determine merge-base with $TARGET_BRANCH${NC}"
            echo "Ensure the target branch is fetched (git fetch origin <branch>)"
            exit 1
        fi
    fi
fi

echo "Validating migration timestamps..."
echo "  Target branch: $TARGET_BRANCH"
echo "  Base SHA: $BASE_SHA"
echo ""

get_latest_timestamp_in_branch() {
    local migration_type="$1"
    local dir="${MIGRATIONS_DIR}/${migration_type}"
    
    git ls-tree -r --name-only "$TARGET_BRANCH" -- "$dir" 2>/dev/null | \
        grep -E '/[0-9]{14}_.*\.go$' | \
        sed 's|.*/||' | \
        grep -oE '^[0-9]{14}' | \
        sort -r | \
        head -1
}

get_new_migration_files() {
    local migration_type="$1"
    local dir="${MIGRATIONS_DIR}/${migration_type}"
    
    git diff --name-only --diff-filter=A "${BASE_SHA}...HEAD" -- "$dir" 2>/dev/null | \
        grep -E '/[0-9]{14}_.*\.go$' || true
}

extract_timestamp() {
    local filepath="$1"
    basename "$filepath" | grep -oE '^[0-9]{14}'
}

errors_found=0

for migration_type in "${MIGRATION_DIRS[@]}"; do
    echo "Checking ${migration_type}..."
    
    latest_in_target=$(get_latest_timestamp_in_branch "$migration_type")
    
    if [[ -z "$latest_in_target" ]]; then
        echo "  No existing migrations found in target branch for ${migration_type}, skipping."
        continue
    fi
    
    echo "  Latest timestamp in target branch: $latest_in_target"
    
    new_migrations=$(get_new_migration_files "$migration_type")
    
    if [[ -z "$new_migrations" ]]; then
        echo -e "  ${GREEN}No new migrations added.${NC}"
        continue
    fi
    
    while IFS= read -r migration_file; do
        [[ -z "$migration_file" ]] && continue
        
        new_timestamp=$(extract_timestamp "$migration_file")
        
        if [[ -z "$new_timestamp" ]]; then
            echo -e "${RED}ERROR: Could not extract timestamp from $migration_file${NC}"
            echo "This indicates a bug in the validation script's parsing logic."
            exit 1
        fi
        
        if [[ "$new_timestamp" < "$latest_in_target" ]]; then
            errors_found=1
            echo ""
            echo -e "${RED}ERROR: Migration timestamp conflict detected!${NC}"
            echo ""
            echo "  Your migration file:"
            echo "    $migration_file"
            echo ""
            echo "  Has timestamp: $new_timestamp"
            echo "  Latest in '$TARGET_BRANCH': $latest_in_target"
            echo ""
            echo -e "${YELLOW}This migration would not run after merge because a newer migration${NC}"
            echo -e "${YELLOW}already exists in the target branch.${NC}"
            echo ""
            echo "To fix this:"
            echo "  1. Rename your migration file with a current timestamp:"
            echo "     mv $migration_file \\"
            echo "        ${MIGRATIONS_DIR}/${migration_type}/\$(date +%Y%m%d%H%M%S)_$(basename "$migration_file" | sed 's/^[0-9]*_//')"
            echo ""
            echo "  2. Update the migration ID inside the file to match the new filename"
            echo ""
            echo "  3. Commit and push the changes"
            echo ""
        elif [[ "$new_timestamp" == "$latest_in_target" ]]; then
            errors_found=1
            echo ""
            echo -e "${RED}ERROR: Duplicate migration timestamp detected!${NC}"
            echo ""
            echo "  Your migration file:"
            echo "    $migration_file"
            echo ""
            echo "  Has the same timestamp ($new_timestamp) as an existing migration in '$TARGET_BRANCH'"
            echo ""
            echo "To fix this, regenerate your migration with a new timestamp."
            echo ""
        else
            echo -e "  ${GREEN}OK:${NC} $migration_file (timestamp: $new_timestamp)"
        fi
    done <<< "$new_migrations"
done

echo ""
if [[ $errors_found -eq 1 ]]; then
    echo -e "${RED}Validation failed: Migration timestamp conflicts detected.${NC}"
    exit 1
else
    echo -e "${GREEN}Validation passed: All migration timestamps are valid.${NC}"
    exit 0
fi

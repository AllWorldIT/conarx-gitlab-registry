#!/usr/bin/env python3
import os
import re
import subprocess
from datetime import datetime, timedelta


def parse_codeowners(codeowners_path):
    """Parse the CODEOWNERS file and return a list of patterns."""
    if not os.path.exists(codeowners_path):
        print(f"Error: CODEOWNERS file not found at {codeowners_path}")
        return []

    patterns = []
    # current_section = None

    with open(codeowners_path, 'r') as f:
        for line in f:
            # Skip empty lines
            line = line.strip()
            if not line:
                continue

            # Skip comments
            if line.startswith('#'):
                continue

            # Check for section headers [Section Name]
            section_match = re.match(r'^\[(.*?)\]', line)
            if section_match:
                # current_section = section_match.group(1).strip()
                # Skip section headers, they're not patterns
                continue

            # Extract the pattern (first part before the owners)
            # Owners start with @ symbol
            parts = line.split('@', 1)

            if len(parts) >= 1:
                # The pattern is everything before the first @ symbol
                pattern = parts[0].strip()

                # Special case: if pattern is exactly "/" it means the whole repo and
                # we do not count it as this is a catch-all for files without owners.
                # Skip empty patterns
                if not pattern or pattern == "/":
                    continue

                # Add pattern to the list
                patterns.append(pattern)

                # Additional debug information (remove in production)
                # print(f"Section: {current_section}, Pattern: {pattern}")

    return patterns


def is_file_covered(file_path, codeowners_patterns):
    """Check if a file is covered by any specific pattern in CODEOWNERS."""
    for pattern in codeowners_patterns:
        regex_pattern = pattern.replace(
            '.', '\\.').replace('*', '.*').replace('?', '.')

        if pattern.startswith('/'):
            regex_pattern = "^" + regex_pattern.lstrip('/')

        if re.search(regex_pattern, file_path):
            return True

    return False


def count_lines_in_file(file_path):
    """Count the number of lines in a file."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Error counting lines in {file_path}: {e}")
        return 0


def get_commits_in_last_year():
    """Get all commits from the last 12 months."""
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    try:
        result = subprocess.run(
            ['git', 'log', '--since', one_year_ago, '--format=%H'],
            capture_output=True, text=True, check=True
        )
        commits = result.stdout.strip().split('\n')
        return [c for c in commits if c]  # Filter out empty strings
    except subprocess.CalledProcessError as e:
        print(f"Error getting commits: {e}")
        return []


def get_files_changed_in_commit(commit_hash):
    """Get all files changed in a specific commit."""
    try:
        result = subprocess.run(
            ['git', 'diff-tree', '--no-commit-id',
                '--name-only', '-r', commit_hash],
            capture_output=True, text=True, check=True
        )
        files = result.stdout.strip().split('\n')
        return [f for f in files if f]  # Filter out empty strings
    except subprocess.CalledProcessError as e:
        print(f"Error getting files for commit {commit_hash}: {e}")
        return []


def get_repo_root():
    """Determine the repository root."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        # If git command fails, return relative path based on script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level from script directory
        return os.path.dirname(script_dir)


def main():
    # Get repository root
    repo_root = get_repo_root()
    print(f"Repository root: {repo_root}")

    # Path to CODEOWNERS file relative to script
    codeowners_path = os.path.normpath(
        os.path.join(repo_root, '.gitlab/CODEOWNERS'))

    print(f"Looking for CODEOWNERS file at: {codeowners_path}")

    # Parse CODEOWNERS file
    codeowners_patterns = parse_codeowners(codeowners_path)
    if not codeowners_patterns:
        print("No valid patterns found in CODEOWNERS file")
        return

    print(f"Found {len(codeowners_patterns)} patterns in CODEOWNERS file")

    # Change directory to repository root
    os.chdir(repo_root)

    # Find all Go files in the repository
    go_files = []
    for root, _, files in os.walk('.'):
        if '.git' in root:
            continue
        for file in files:
            if file.endswith('.go') and not file.endswith('_test.go'):
                file_path = os.path.join(root, file)
                go_files.append(file_path)

    # Count lines of code and check coverage
    total_loc = 0
    covered_loc = 0
    covered_files = []
    uncovered_files = []

    for file_path in go_files:
        # Normalize path for pattern matching
        normalized_path = file_path
        if normalized_path.startswith('./'):
            normalized_path = normalized_path[2:]

        # Count lines
        loc = count_lines_in_file(file_path)
        total_loc += loc

        # Check if covered by CODEOWNERS
        if is_file_covered(normalized_path, codeowners_patterns):
            covered_loc += loc
            covered_files.append(normalized_path)
        else:
            uncovered_files.append(normalized_path)

    # Get commits in the last 12 months
    commits = get_commits_in_last_year()
    total_commits = len(commits)

    # Count commits touching uncovered files
    uncovered_commits = set()
    for commit in commits:
        changed_files = get_files_changed_in_commit(commit)
        # Normalize paths
        changed_files = [f if not f.startswith(
            './') else f[2:] for f in changed_files]
        # Filter for Go files only
        changed_go_files = [f for f in changed_files if f.endswith('.go')]

        # Check if any uncovered file was touched
        for changed_file in changed_go_files:
            if changed_file in uncovered_files:
                uncovered_commits.add(commit)
                break

    # Print results
    print(f"Total Go files: {len(go_files)}")
    print(
        f"Files covered by CODEOWNERS: {len(covered_files)} ({len(covered_files)/len(go_files)*100:.2f}%)")
    print(
        f"Files not covered by CODEOWNERS: {len(uncovered_files)} ({len(uncovered_files)/len(go_files)*100:.2f}%)")
    print()
    print(f"Total lines of Go code: {total_loc}")
    print(
        f"Lines covered by CODEOWNERS: {covered_loc} ({covered_loc/total_loc*100:.2f}%)")
    print(
        f"Lines not covered by CODEOWNERS: {total_loc - covered_loc} ({(total_loc - covered_loc)/total_loc*100:.2f}%)")
    print()
    print(f"Total commits in the last 12 months: {total_commits}")
    uncovered_commit_count = len(uncovered_commits)
    print(
        f"Commits touching files not covered by CODEOWNERS: {uncovered_commit_count} ({uncovered_commit_count/total_commits*100:.2f}%)")


if __name__ == "__main__":
    main()

#!/usr/bin/env bash

set -o pipefail

GIT_ROOT="$(dirname "$(realpath "$0")")/../"
ERROR_RESULTS=0

# We are runing in a busybox docker container with very limited tooling, hence
# print0
FILES=$(find "${GIT_ROOT}/docs/" -type f -name "*.md" -print)

echo "Lint prose"
if command -v vale >/dev/null 2>&1; then
    echo $FILES | xargs vale --config "${GIT_ROOT}/.vale.ini" || ((ERROR_RESULTS++))
else
    echo "Vale is missing, please install it from https://vale.sh/docs/vale-cli/installation/"
fi

echo "Lint Markdown"
if command -v markdownlint-cli2 >/dev/null 2>&1; then
    echo $FILES | xargs markdownlint-cli2 || ((ERROR_RESULTS++))
else
    echo "markdownlint-cli2 is missing, please install it from https://github.com/DavidAnson/markdownlint-cli2#install"
fi

echo "Check links"
if command -v lychee >/dev/null 2>&1; then
   echo $FILES | xargs lychee --offline --include-fragments || ((ERROR_RESULTS++))
else
    echo "Lychee is missing, please install it from https://lychee.cli.rs/installation/"
fi

if [ "${ERROR_RESULTS}" -ne 0 ]; then
    echo "✖ ${ERROR_RESULTS} lint test(s) failed. Review the log carefully to see full listing."
    exit 1
else
    echo "✔ Linting passed"
    exit 0
fi

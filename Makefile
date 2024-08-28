# Root directory of the project (absolute path).
ROOTDIR=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))

GOLANGCI_VERSION ?= v1.59.1

# Used to populate version variable in main package.
VERSION?=$(shell git describe --tags --match 'v[0-9]*' --dirty='.m' --always)
REVISION?=$(shell git rev-parse HEAD)$(shell if ! git diff --no-ext-diff --quiet --exit-code; then echo .m; fi)
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%S")

PKG=github.com/docker/distribution

# Project packages.
PACKAGES=$(shell go list -tags "${BUILDTAGS}" ./...)
INTEGRATION_PACKAGE=${PKG}
COVERAGE_PACKAGES=$(filter-out ${PKG}/registry/storage/driver/%,${PACKAGES})
GO_TEST ?='go test'


# Project binaries.
COMMANDS=registry digest

# Allow turning off function inlining and variable registerization
ifeq (${DISABLE_OPTIMIZATION},true)
	GO_GCFLAGS=-gcflags "-N -l"
	VERSION:="$(VERSION)-noopt"
endif

WHALE = "+"

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))

# Go files
#
TESTFLAGS_RACE=
GOFILES=$(shell find . -type f -name '*.go')
GO_TAGS=$(if $(BUILDTAGS),-tags "$(BUILDTAGS)",)
GO_LDFLAGS=-ldflags '-s -w -X $(PKG)/version.Version=$(VERSION) -X $(PKG)/version.Revision=$(REVISION) -X $(PKG)/version.Package=$(PKG) -X $(PKG)/version.BuildTime=$(BUILD_TIME) $(EXTRA_LDFLAGS)'

BINARIES=$(addprefix bin/,$(COMMANDS))

# Flags passed to `go test`
TESTFLAGS ?= -v $(TESTFLAGS_RACE)
TESTFLAGS_PARALLEL ?= 8

.PHONY: all build binaries check clean test test-race test-full integration coverage dev-tools release-dry-run release
.DEFAULT: all

all: binaries

check: lint

lint: ## run golangci-lint, with defaults
	@echo "$(WHALE) $@"
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@${GOLANGCI_VERSION} run

lint-docs: ## run golangci-lint, with defaults
	@#There are few issues with installing the tooling natively:
	@# * lychee is not available for asdf and we do not use asdf in our script in the first place
	@# * some of us are on Mac (brew), some of us are on Linux (apt/cargo/snap/...)
	@# * only markdownlint-cli2 can be easily installed using npm
	@# TODO(prozlach): the docker image name+tag should be extracted directly from 
	@echo "$(WHALE) $@"
	@docker run \
		-w /root/repo/ \
		-v ${MAKEFILE_DIR}:/root/repo/ \
		registry.gitlab.com/gitlab-org/gitlab-docs/lint-markdown:alpine-3.20-vale-3.6.1-markdownlint2-0.13.0-lychee-0.15.1 \
			script/lint-docs.sh

test: ## run tests, except integration test with test.short
	@echo "$(WHALE) $@"
	@go test ${GO_TAGS} -test.short ${TESTFLAGS} $(filter-out ${INTEGRATION_PACKAGE},${PACKAGES})

test-race: ## run tests, except integration test with test.short and race
	@echo "$(WHALE) $@"
	@go test ${GO_TAGS} -race -test.short ${TESTFLAGS} $(filter-out ${INTEGRATION_PACKAGE},${PACKAGES})

test-full: ## run tests, except integration tests
	@echo "$(WHALE) $@"
	@go test ${GO_TAGS} ${TESTFLAGS} $(filter-out ${INTEGRATION_PACKAGE},${PACKAGES})

integration: ## run integration tests
	@echo "$(WHALE) $@"
	@go test ${TESTFLAGS} -parallel ${TESTFLAGS_PARALLEL} ${INTEGRATION_PACKAGE}

coverage:
	@echo "$(WHALE) $@"
	@rm -f coverage.txt
	@$(subst $\',,$(GO_TEST)) -coverprofile=coverage.txt -covermode=atomic ${GO_TAGS} ${TESTFLAGS} $(filter-out ${INTEGRATION_PACKAGE},${COVERAGE_PACKAGES})
	@go tool cover -func coverage.txt

FORCE:

# Build a binary from a cmd.
bin/%: cmd/% FORCE
	@echo "$(WHALE) $@${BINARY_SUFFIX}"
	@go build ${GO_GCFLAGS} ${GO_BUILD_FLAGS} -o $@${BINARY_SUFFIX} ${GO_LDFLAGS} ${GO_TAGS}  ./$<

binaries: $(BINARIES) ## build binaries
	@echo "$(WHALE) $@"

build:
	@echo "$(WHALE) $@"
	@go build ${GO_GCFLAGS} ${GO_BUILD_FLAGS} ${GO_LDFLAGS} ${GO_TAGS} $(PACKAGES)

clean: ## clean up binaries
	@echo "$(WHALE) $@"
	@rm -f $(BINARIES)

db-new-migration:
	@./script/dev/db-new-migration $(filter-out $@,$(MAKECMDGOALS))
%:
	@:

db-structure-dump:
	@./script/dev/db-structure-dump

dev-tools:
	npm install -g \
		@commitlint/cli@17 \
		@commitlint/config-conventional@17 \
		semantic-release@21 \
		@semantic-release/commit-analyzer@10 \
		@semantic-release/release-notes-generator@11 \
		@semantic-release/changelog@6 \
		@semantic-release/git@10 \
		conventional-changelog-conventionalcommits@6
	go install go.uber.org/mock/mockgen@v0.4.0

# https://github.com/semantic-release/git#environment-variables
export GIT_AUTHOR_NAME="$(shell git config user.name)"
export GIT_AUTHOR_EMAIL=$(shell git config user.email)
export GIT_COMMITTER_NAME="$(shell git config user.name)"
export GIT_COMMITTER_EMAIL=$(shell git config user.email)

release-prep:
	@git checkout master
	@git pull origin master

release-dry-run: release-prep
	@npx semantic-release --no-ci --dry-run

release: release-prep
	@echo "This will generate and push a changelog update and a new tag, are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
	@npx semantic-release --no-ci

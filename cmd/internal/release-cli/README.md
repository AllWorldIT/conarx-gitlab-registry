# Container Registry Release CLI

This README is pertaining to the `release-cli` tool used by the Container Registry team
to release to various projects within GitLab. To find more about the release process
refer to the [Release Plan](https://gitlab.com/gitlab-org/container-registry/-/blob/master/.gitlab/issue_templates/Release%20Plan.md)
issue template or the [release section](https://gitlab.com/gitlab-org/container-registry/-/blob/master/CONTRIBUTING.md#releases)
in the contributing guide.

## How to use

The `release-cli` runs on a release CI pipeline when a new tag is pushed to
this project. The following commands are available from the root of this project:

```shell
Release a new version of Container Registry on the specified project

Usage:
  release  [command]

Available Commands:
  charts      Release to Cloud Native GitLab Helm Chart
  cng         Release to Cloud Native container images components of GitLab
  gdk         Release to GitLab Development Kit
  issue       Create a Release Plan issue
  k8s         Release to Kubernetes Workload configurations for GitLab.com
  omnibus     Release to Omnibus GitLab

Flags:
  -h, --help   help for release

Use "release [command] --help" for more information about a command.
```

**Note:** The `release-cli` is meant to be run in a CI context and not locally
as its implementation depends on 
[CI predefined variables](https://docs.gitlab.com/ee/ci/variables/predefined_variables.html). 

## Configuration

The configuration file used by the `release-cli` can be found at [`config/config.yaml`](https://gitlab.com/gitlab-org/container-registry/-/blob/master/cmd/internal/release-cli/.config.yaml). This file
describes not only which projects to release to and files to change, but also allows customisations such as commit messages, MR title and branch name.

The `$TARGET_AUTH_TOKEN` used is an auth token with sufficient permissions to 
post a merge request on the project that we are releasing to and `$TARGET_TRIGGER_TOKEN` is required to trigger pipelines on projects we release to, where the GitLab Dependency Bot is responsible to update the versions. There is also a `$SOURCE_AUTH_TOKEN` which has sufficient permissions to read the release notes from the Container Registry project.

## Maintenance

Due to the way the `release-cli` tool updates files, it is important to be aware of major breaking changes
on files that need to be updated for a release. These files are explicitly stated in the `config/config.yaml`.

The CNG, Charts and Omnibus release commands make use of the the GitLab Dependency Bot, the bot user that the GitLab Distribution team uses for automatically submitting MRs with dependency updates using https://docs.renovatebot.com/.
The file changes are stated in the renovate-gitlab-bot repository, under the directory for the release target project, like [this](https://gitlab.com/gitlab-org/frontend/renovate-gitlab-bot/-/blob/main/renovate/distribution/omnibus.config.js). For more information refer to [Renovate GitLab Bot - 101](https://gitlab.com/gitlab-org/distribution/distributions-101/-/tree/main/GitLab%20Renovate%20Bot).

We also require a few secrets to be set as CI Variables that are necessary for triggering a specific release following the `$BUMP_VERSION_TRIGGER_TOKEN_<PROJECT>` pattern if it is a [trigger token](https://docs.gitlab.com/ee/ci/triggers/#create-a-trigger-token) or `$BUMP_VERSION_AUTH_TOKEN_<PROJECT>` if it is an auth token.

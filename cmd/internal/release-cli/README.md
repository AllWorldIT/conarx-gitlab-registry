# Container Registry Release CLI

This README is pertaining to the `release-cli` tool used by the Container Registry team
to release to various projects within GitLab. To find more about the release process
refer to the [Release Plan](https://gitlab.com/gitlab-org/container-registry/-/blob/master/.gitlab/issue_templates/Release%20Plan.md) issue template or the [release section](https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/README.md#releases) in the Readme.

## How to use

The `release-cli` runs on a release CI pipeline when a new tag is pushed to
this project. The following commands are available from the root of this project:

```bash
Release a new version of Container Registry on the specified project

Usage:
  release  [flags]
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

Global Flags:
      --auth-token string   Auth token with permissions to open MRs on the project to release to
      --config string       Config file (default is $HOME/.config.yaml)
      --tag string          Release version

Use "release [command] --help" for more information about a command.
```

Releasing to CNG, Charts and Omnibus require an additional flag

```bash
 --trigger-token string   Trigger token for pipeline trigering
```

with a [trigger token](https://docs.gitlab.com/ee/ci/triggers/#create-a-trigger-token) from the project to release to.

**Note:** The `release-cli` is meant to be run in a CI context and not locally
as its implementation depends on 
[CI predefined variables](https://docs.gitlab.com/ee/ci/variables/predefined_variables.html). 

## Configuration

The configuration file used by the `release-cli` can be found at [`.config.yaml`](https://gitlab.com/gitlab-org/container-registry/-/blob/master/cmd/internal/release-cli/.config.yaml). This file
describes not only which projects to release to and files to change, but also allows customisations such as commit messages, MR title and branch name.

It also leverages CI Variables available in `.gitlab-ci.yml` such as the `$CI_COMMIT_TAG` 
to get the tag used in the release context. The `$AUTH_TOKEN` used is an auth token with sufficient permissions to 
post a merge request on the project that we are releasing to and `$TRIGGER_TOKEN` is required to trigger pipelines on projects
where the GitLab Dependency Bot is responsible to update the versions.

## Maintenance

Due to the way the `release-cli` tool updates files, it is important to be aware of major breaking changes
on files that need to be updated for a release. These files are explicilty stated in the `.config.yaml` under
the `filenames` key. 

The CNG, Charts and Omnibus release commands make use of the the GitLab Dependency Bot, the bot user that the GitLab Distribution team uses for automatically submitting MRs with dependency updates using https://www.dependencies.io/.
The file changes are stated on the `deps.yml` file of the release project, like [this](https://gitlab.com/gitlab-org/build/CNG/-/blob/master/deps.yml#L87).

We also require a few secrets to be set as CI Variables that are necessary for triggering a specific release following the `$BUMP_VERSION_TRIGGER_TOKEN_<PROJECT>` pattern if it is a [trigger token](https://docs.gitlab.com/ee/ci/triggers/#create-a-trigger-token) or `$BUMP_VERSION_AUTH_TOKEN_<PROJECT>` if it is an auth token.

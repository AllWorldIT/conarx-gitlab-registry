<!--
Please use the following format for the issue title:

Release Version vX.Y.Z-gitlab

Example:

Release Version v2.7.7-gitlab
-->

## What's New in this Version

<!--
* Copy the changelog description from https://gitlab.com/gitlab-org/container-registry/-/blob/master/CHANGELOG.md that corresponds to this release, adjusting the headers to `###` for the version diff and `####` for the change categories.

Example:

### [3.43.0](https://gitlab.com/gitlab-org/container-registry/compare/v3.42.0-gitlab...v3.43.0-gitlab) (2022-05-20)


#### Bug Fixes

* gracefully handle missing manifest revisions during imports ([bc7c43f](https://gitlab.com/gitlab-org/container-registry/commit/bc7c43f30d8aba8f2edf2ca741b366614d9234c3))


#### Features

* add ability to check/log whether FIPS crypto has been enabled ([1ac2454](https://gitlab.com/gitlab-org/container-registry/commit/1ac2454ac9dc7eeca5d9b555e0f1e6830fa66439))
* add support for additional gardener media types ([10153f8](https://gitlab.com/gitlab-org/container-registry/commit/10153f8df9a147806084aaff0f95a9d9536bbbe5))
-->

[copy changelog here]

## Tasks

All tasks must be completed (in order) for the release to be considered ~"workflow::production".

### 1. Prepare

1. [ ] Set the milestone of this issue to the target GitLab release.
1. [ ] Set the due date of this issue to 10 days before the [date of the target GitLab release](https://about.gitlab.com/releases/#upcoming-releases)

<details>
<summary><b>Documentation/resources</b></summary>

The due date is set to 10 days before the targeted GitLab release date to create a buffer of 5 days before the merge deadline.
See [Product Development Timeline](https://about.gitlab.com/handbook/engineering/workflow/#product-development-timeline) for more information about the GitLab release timings.

</details>

### 2. Release

1. [ ] Run the `make release-dry-run` command.
1. [ ] Review each MR in the new release and check if the ~"cannot-rollback" or the ~"high-risk-change" label has been applied. If any MR contains the label:
   1. [ ] Ensure that _no_ code changes that rely on the ~"cannot-rollback" MRg are included in this release. These should be separated into two consecutive releases.
1. [ ] Run the `make release` command. A new tag should have been created and pushed.

<details>
<summary><b>Documentation/resources</b></summary>

The release documentation can be found [here](https://gitlab.com/gitlab-org/container-registry/-/blob/master/CONTRIBUTING.md#releases).

</details>


### 3. Update


1. [ ] The version bump for [CNG](https://gitlab.com/gitlab-org/build/CNG) is automatically created by the renovate bot, which is triggered every 15-30 minutes.
   1. [ ] Check for the renovate MR [here](https://gitlab.com/gitlab-org/build/CNG/-/merge_requests?scope=all&state=opened&label_name[]=automation%3Abot-authored&search=container-registry). Once the MR is created:
      1. [ ] Mark it as related to this release issue.
      1. [ ] Either request a review from `@gitlab-org/maintainers/container-registry` to speed up the process, or just let the bot pick a Distribution reviewer. If reviewing the MR, make sure:
         - [ ] The MR is targeting the `master` branch.
         - [ ] The MR has a green pipeline on GitLab.com.
1. [ ] The version bump for [GDK](https://gitlab.com/gitlab-org/gitlab-development-kit/-/merge_requests) needs to be done manually ([example](https://gitlab.com/gitlab-org/gitlab-development-kit/-/merge_requests/4247)) as the CI job is currently not functioning.
   - [ ] Assign to the reviewer suggested by reviewer roulette
1. [ ] The version bump for [Omnibus](https://gitlab.com/gitlab-org/omnibus-gitlab) is automatically created by the renovate bot, which is triggered every 15-30 minutes.
    1. [ ] Check for the renovate MR [here](https://gitlab.com/gitlab-org/omnibus-gitlab/-/merge_requests?scope=all&state=opened&label_name[]=automation%3Abot-authored&search=container-registry). Once the MR is created:
        1. [ ] Mark it as related to this release issue;
        1. [ ] Let the bot pick a Distribution reviewer.
1. [ ] The version bump for [Charts](https://gitlab.com/gitlab-org/charts/gitlab) is automatically created by the renovate bot, which is triggered every 15-30 minutes.
    1. [ ] Check for the renovate MR [here](https://gitlab.com/gitlab-org/charts/gitlab/-/merge_requests?scope=all&state=opened&label_name[]=automation%3Abot-authored&search=container-registry). Once the MR is created:
        1. [ ] Mark it as related to this release issue;
        1. [ ] Let the bot pick a Distribution reviewer.
1. [ ] Version bumps in [K8s Workloads](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com) need to be done manually for now as CI is broken. The MR title should be "Bump Container Registry to [version] ([environment(s)])".
   1. [ ] Wait for the CNG version bump to be merged.
   1. [ ] Check MRs included in the release for the labels ~high-risk-change, ~cannot-rollback.
      - [ ] If they exist, add the same label to each deployment stage.
      - [ ] Follow the [potentially risky deployments](#potentially-risky-deployments) instructions.
   1. Each environment needs to be deployed and confirmed working in the order listed below, before merging the next MR. To see the version deployed in each environment, look at the [versions chart in Grafana](https://dashboards.gitlab.net/goto/F44DoeCIg?orgId=1)
      1. [ ] Version bump for Pre-Production and Staging.
      1. [ ] Version bump for Production Canary.
      1. [ ] Version bump for Production Main Stage.
1. [ ] If this is the last registry release in the milestone, open an MR to update [`REGISTRY_SELF_MANAGED_RELEASE_VERSION`](https://gitlab.com/gitlab-org/container-registry/-/blob/master/.gitlab/ci/migrate.yml?ref_type=heads#L9) to the new self-managed release version.

#### Potentially risky deployments

<details>
<summary><b>Instructions</b></summary>

1. Add the following instructions to each deployment MR.

   - [ ] Version bump for Pre-Production and Staging.
     - [ ] Check the [`#qa-staging` Slack channel](https://gitlab.slack.com/archives/CBS3YKMGD) for `staging end-to-end tests passed!`. Make sure the corresponding pipeline started _after_ the registry deployment completed. Otherwise, wait for the next one.
     - [ ] Check [logs](https://nonprod-log.gitlab.net/goto/f3fbccdb9dea6805ff5bbf1e0144a04e) for errors.
     - [ ] Check [metrics dashboard](https://dashboards.gitlab.net/d/registry-main/registry-overview?orgId=1&var-PROMETHEUS_DS=Global&var-environment=gstg&var-stage=main).
   - [ ] Version bump for Production Canary.
     - [ ] Check the [`#qa-production` Slack channel](https://gitlab.slack.com/archives/CCNNKFP8B) for `canary end-to-end tests passed!`.
     - [ ] Check [logs](https://log.gprd.gitlab.net/goto/9a66e350-fea0-11ed-a017-0d32180b1390) for errors (`json.stage: cny`).
     - [ ] Check [metrics dashboard](https://dashboards.gitlab.net/d/registry-main/registry-overview?orgId=1&var-PROMETHEUS_DS=Global&var-environment=gprd&var-stage=cny).
   - [ ] Version bump for Production Main Stage.
     - [ ] Check the [`#qa-production` Slack channel](https://gitlab.slack.com/archives/CCNNKFP8B) for `production end-to-end tests passed!`. Make sure the corresponding pipeline started _after_ the registry deployment completed. Otherwise, wait for the next one.
     - [ ] Check [logs](https://log.gprd.gitlab.net/goto/7dc6f73d5dd4cc4bebcd4af3b767cae4) for errors.
     - [ ] Check [metrics dashboard](https://dashboards.gitlab.net/d/registry-main/registry-overview?orgId=1&var-PROMETHEUS_DS=Global&var-environment=gprd&var-stage=main).

2. Let the assignee SRE know about these changes.

</details>

### 4. Complete

1. [ ] Assign label ~"workflow::verification" once all changes have been merged.
1. [ ] Assign label ~"workflow::production" once all changes have been deployed.
1. Close this issue.

/label ~"devops::package" ~"section::ci" ~"group::container registry" ~"Category:Container Registry" ~golang ~"workflow::in dev" ~"type::maintenance" ~"maintenance::release"

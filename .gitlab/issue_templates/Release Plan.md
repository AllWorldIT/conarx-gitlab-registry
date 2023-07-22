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
1. [ ] Set the due date of this issue to the 12th of the release month.

<details>
<summary><b>Instructions</b></summary>
The due date is set to the 12th of each month to create a buffer of 5 days before the merge deadline on the 17th. See [Product Development Timeline](https://about.gitlab.com/handbook/engineering/workflow/#product-development-timeline) for more information about the GitLab release timings.
</details>

### 2. Release

1. Read the [release documentation](https://gitlab.com/gitlab-org/container-registry/-/tree/master/docs-gitlab#releases).
1. Run the `make release-dry-run` command.
1. Review each MR and check if the ~"cannot-rollback" or the ~"high-risk-change" label has been applied. If any MR contains the label:
   1. Ensure that _no_ code changes that rely on the ~"cannot-rollback" MRg are included in this release. These should be separated into two consecutive releases.
1. Run the `make release` command.
1. A new tag should have been created and pushed.


### 3. Update

**Note:** Version bumps for CNG, Omnibus and GDK can be triggered at the "same" time. Only the Charts and K8s Workloads
bumps need to wait for the CNG one. It's highly recommended to trigger both CNG and Omnibus bumps together so that the
Distribution team can see them while reviewing.

1. [ ] Version bump in [CNG](https://gitlab.com/gitlab-org/build/CNG) is automatically done using the internal `release-cli`. An MR should be found open on the [CNG MR page](https://gitlab.com/gitlab-org/build/CNG/-/merge_requests) after manually triggering the `version-bump:cng` job. If opening this MR manually please give it the title "Bump Container Registry to [version]".
1. [ ] Version bump in [Omnibus](https://gitlab.com/gitlab-org/omnibus-gitlab) is automatically done using the internal `release-cli`. An MR should be found open on the [Omnibus MR page](https://gitlab.com/gitlab-org/omnibus-gitlab/-/merge_requests) after manually triggering the `version-bump:omnibus` job. If opening this MR manually please give it the title "Bump Container Registry to [version]".
1. [ ] Version bump in [Charts](https://gitlab.com/gitlab-org/charts) is automatically done using the internal `release-cli`. An MR should be found open on the [Charts MR page](https://gitlab.com/groups/gitlab-org/charts/-/merge_requests) after manually triggering the `version-bump:charts` job (which requires `version-bump:cng` to be triggered first). If opening this MR manually please give it the title "Bump Container Registry to [version]".
1. [ ] Version bumps in [K8s Workloads](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com) are automatically done using the internal `release-cli`. There should be three separate MRs, listed below, on the [K8s Workloads MR page](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com/-/merge_requests) after manually triggering the `version-bump:k8s` job (which requires `version-bump:cng` to be triggered first). Each environment needs to be deployed and confirmed working in the order listed below, before merging the next MR. If opening this MR manually please give it the title "Bump Container Registry to [version] ([environment(s)])".
    - [ ] Check MRs included in the release for the labels ~high-risk-change, ~cannot-rollback.
       - [ ] If they exist, add the same label to each deployment stage.
       - [ ] Follow the [potentially risky deployments](#potentially-risky-deployments) instructions.
    - [ ] Version bump for Pre-Production and Staging.
    - [ ] Version bump for Production Canary.
    - [ ] Version bump for Production Main Stage.
1. [ ] Version bump for [GDK](https://gitlab.com/gitlab-org/gitlab-development-kit) is automatically done using the internal `release-cli`. An MR should be found open on the [GDK MR page](https://gitlab.com/gitlab-org/gitlab-development-kit/-/merge_requests) after manually triggering the `version-bump:gdk` job. If opening this MR manually please give it the title "Bump Container Registry to [version]".
   - [ ] Assign to the reviewer suggested by reviewer roulette

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

#### Release instructions

<details>
<summary><b>Instructions</b></summary>

Bump the Container Registry version used in [CNG](https://gitlab.com/gitlab-org/build/CNG), [Omnibus](https://gitlab.com/gitlab-org/omnibus-gitlab), [Charts](https://gitlab.com/gitlab-org/charts) and [K8s Workloads](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com) by manually triggering these on the `release` job.

The CNG image is the pre-requisite for the remaining version bumps which may be merged independently from each other. Only CNG and K8s Workloads version bumps are required for a GitLab.com deployment. The deployment is then completed as documented [here](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com/-/blob/master/DEPLOYMENT.md). Charts and Omnibus version bumps are required for self-managed releases.

Please mark parent tasks as completed once the corresponding merge requests are merged.

Version bump merge requests should appear automatically in the `Related merge requests` section of this issue.

Note: According to the [Distribution Team Merge Request Handling](https://about.gitlab.com/handbook/engineering/development/enablement/distribution/merge_requests.html#assigning-merge-requests) documentation, we should not assign merge requests to an individual.

#### Merge Request Template

For consistency when updating manually, please use the following template for these merge requests:

##### Branch Name

`bump-container-registry-vX-Y-Z-gitlab`

##### Commit Message

```
Bump Container Registry to vX.Y.Z-gitlab

Changelog: changed
```

##### Title

`Bump Container Registry to vX.Y.Z-gitlab`

##### Description

Repeat the version subsection for multiple versions. As an example, to bump to v2.7.7 in a project where the current version is v2.7.5, create an entry for v2.7.6 and v2.7.7.

```md
## vX.Y.Z-gitlab

[Changelog](https://gitlab.com/gitlab-org/container-registry/blob/release/X.Y-gitlab/CHANGELOG.md#vXYZ-gitlab-YYYY-MM-DD)

Related to <!-- link to this release issue -->.
```

</details>

### 4. Complete

1. [ ] Assign label ~"workflow::verification" once all changes have been merged.
1. [ ] Assign label ~"workflow::production" once all changes have been deployed.
1. [ ] Update all related issues, informing that the deploy is complete.
1. [ ] Close this issue.

<details>
<summary><b>Instructions</b></summary>
To see the version deployed in each environment, look at the [Grafana Container Registry dashboard](https://dashboards.gitlab.net/d/registry-pod/registry-pod-info?orgId=1):

![image](/uploads/3fd5b4902472f6cdcc56b9c2d333472f/image.png)

/label ~"devops::package" ~"group::container registry" ~"Category:Container Registry" ~golang ~"workflow::in dev" ~"type::maintenance" ~"maintenance::release"

</details>

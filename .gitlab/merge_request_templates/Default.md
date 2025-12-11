## What does this MR do?

<!-- Describe your changes here -->

%{first_multiline_commit}

Related to <!-- add the issue URL here -->

## Author checklist

- Assign one of [conventional-commit](https://www.conventionalcommits.org/en/v1.0.0/#summary) prefixes to the MR.
  - [ ] `fix`: Indicates a bug fix, triggers a patch release.
  - [ ] `feat`: Signals the introduction of a new feature, triggers a minor release.
  - [ ] `perf`: Focuses on performance improvements that don't introduce new features or fix bugs, triggers a patch release.
  - [ ] `docs`: Updates or changes to documentation. Does not trigger a release.
  - [ ] `style`: Changes that do not affect the code's functionality. Does not trigger a release.
  - [ ] `refactor`: Modifications to the code that do not fix bugs or add features but improve code structure or readability. Does not trigger a release.
  - [ ] `test`: Changes related to adding or modifying tests. Does not trigger a release.
  - [ ] `chore`: Routine tasks that don't affect the application, such as updating build processes, package manager configs, etc. Does not trigger a release.
  - [ ] `build`: Changes that affect the build system or external dependencies. May trigger a release.
  - [ ] `ci`: Modifications to continuous integration configuration files and scripts. Does not trigger a release.
  - [ ] `revert`: Reverts a previous commit. It could result in a patch, minor, or major release.
- [ ] MR contains ~database changes including schema/background migrations:
  - **Do not** include code that depends on the schema migrations in the same commit. Split the MR into two or more.
  - **Do not** include code that depends on [background migrations](../../docs/spec/gitlab/database-background-migrations.md#work-function-not-found-error) in the same release.
  - [ ] Manually run up and down migrations in a [postgres.ai](https://console.postgres.ai/gitlab/joe-instances/68) production database clone and add a link for the query plan(s) to the MR.
  - [ ] If adding new schema migrations make sure the `REGISTRY_SELF_MANAGED_RELEASE_VERSION` CI variable in [migrate.yml](../ci/migrate.yml) is pointing to the latest GitLab self-managed released registry version. Find the correct registry version [here](https://gitlab.com/gitlab-org/omnibus-gitlab/-/blob/master/config/software/registry.rb?ref_type=heads#L22). Make sure to select the branch of the latest GitLab release.
  - [ ] If adding new queries, extract a query plan from [postgres.ai](https://console.postgres.ai/gitlab/joe-instances/68) and post the link here. If changing existing queries, also extract a query plan for the current version for comparison.
    - [ ] I do not have access to postgres.ai and have made a comment on this MR asking for these to be run on my behalf.
  - [ ] If adding new background migration, follow the guide for [performance testing new background migrations](../../docs/spec/gitlab/database-background-migrations.md#performance-testing-guide) and add a report/summary to the MR with your analysis.
- [ ] Change contains a breaking change - apply the ~"breaking change" label.
- [ ] Change is considered high risk - apply the label ~high-risk-change
- [ ] I created or linked to an existing issue for every added or updated `TODO`, `BUG`, `FIXME` or `OPTIMIZE` prefixed comment
- [ ] Changes cannot be rolled back
  - [ ] Apply the label ~"cannot-rollback".
  - [ ] Add a section to the MR description that includes the following details:
     - [ ] The reasoning behind why a release containing the presented MR can not be rolled back (e.g. schema migrations or changes to the FS structure) 
     - [ ] Detailed steps to revert/disable a feature introduced by the same change where a migration cannot be rolled back. (**note**: ideally MRs containing schema migrations should not contain feature changes.)
     - [ ] Ensure this MR does not add code that depends on these changes that cannot be rolled back.


</details>

<details><summary><b>Documentation/resources</b></summary>

[Code review guidelines](https://docs.gitlab.com/ee/development/code_review.html)

[Go Style guidelines](https://docs.gitlab.com/ee/development/go_guide/)

[Feature flags](https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/feature-flags.md)

[When documentation is required](https://about.gitlab.com/handbook/engineering/ux/technical-writing/workflow/#when-documentation-is-required)

[Documentation workflow](https://docs.gitlab.com/ee/development/documentation/workflow.html)


</details>

## Reviewer checklist

- [ ] Ensure the commit and MR tittle are still accurate.
- [ ] If the change contains a breaking change, verify the ~"breaking change" label.
- [ ] If the change is considered high risk, verify the label ~high-risk-change
- [ ] Identify if the change can be rolled back safely. (**note**: all other reasons for not being able to rollback will be sufficiently captured by major version changes).


<!-- Labels - do not remove -->
/label ~"section::ops" ~"devops::package" ~"group::container registry" ~"Category:Container Registry" ~backend ~golang

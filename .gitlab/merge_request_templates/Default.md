## What does this MR do?

<!-- Describe your changes here -->

%{first_multiline_commit}

Related to <!-- add the issue URL here -->

## Author checklist

- [Feature flags](https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/feature-flags.md)
    - [ ] Added feature flag: <!-- Add the Feature flag tracking issue link here -->
    - [ ] This feature does not require a feature flag
- [ ] I added unit tests or they are not required
- [ ] I added [documentation](https://docs.gitlab.com/ee/development/documentation/workflow.html) ([or it's not required](https://about.gitlab.com/handbook/engineering/ux/technical-writing/workflow/#when-documentation-is-required))
- [ ] I followed [code review guidelines](https://docs.gitlab.com/ee/development/code_review.html)
- [ ] I followed [Go Style guidelines](https://docs.gitlab.com/ee/development/go_guide/)
- [ ] For ~database changes including schema migrations:
   - [ ] Manually run up and down migrations in a [postgres.ai](https://console.postgres.ai/gitlab/joe-instances/68) production database clone and post a screenshot of the result here.
   - [ ] If adding new queries, extract a query plan from [postgres.ai](https://console.postgres.ai/gitlab/joe-instances/68) and post the link here. If changing existing queries, also extract a query plan for the current version for comparison.
   - [ ] **Do not** include code that depends on the schema migrations in the same commit. Split the MR into two or more.
- [ ] Ensured this change is safe to deploy to individual stages in the same environment (`cny` -> `prod`). State-related changes can be troublesome due to having parts of the fleet processing (possibly related) requests in different ways.

## Reviewer checklist

- [ ] Ensure the commit and MR tittle are still accurate.
- [ ] If the change contains a breaking change, apply the ~"breaking change" label.
- [ ] If the change is considered high risk, apply the label ~high-risk-change
- [ ] Identify if the change can be rolled back safely. (**note**: all other reasons for not being able to rollback will be sufficiently captured by major version changes).

If the MR introduces ~database schema migrations:

- [ ] Ensure the commit and MR tittle start with `fix:`, `feat:`, or `perf:` so that the change appears on the [Changelog](https://gitlab.com/gitlab-org/container-registry/-/blob/master/CHANGELOG.md)

<details><summary>If the changes cannot be rolled back follow these steps: </summary>

- [ ] If not, apply the label ~"cannot-rollback".
- [ ] Add a section to the MR description that includes the following details:
   - [ ] The reasoning behind why a release containing the presented MR can not be rolled back (e.g. schema migrations or changes to the FS structure) 
   - [ ] Detailed steps to revert/disable a feature introduced by the same change where a migration cannot be rolled back. (**note**: ideally MRs containing schema migrations should not contain feature changes.)
   - [ ] Ensure this MR does not add code that depends on these changes that cannot be rolled back.

</details>

<!-- Labels - do not remove -->
/label ~"section::ops" ~"devops::package" ~"group::container registry" ~"Category:Container Registry" ~backend ~golang

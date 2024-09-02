# Becoming a GitLab Container Registry maintainer

This document serves as a guideline for GitLab team members that want to become maintainers for the Container Registry project.
Maintainers should have an advanced understanding of the GitLab Container Registry codebase.
Prior to applying for maintainership of a project, a person should gain a good feel for the codebase, expertise in one or more functionalities,
and deep understanding of our coding standards.

## Expectations

The process to [become a maintainer at GitLab is defined in the handbook](https://about.gitlab.com/handbook/engineering/workflow/code-review/#how-to-become-a-project-maintainer),
and it is the baseline for this process. One thing that is expected is a high number of reviews, however; the rate of change of the Container Registry compared to the
GitLab Rails project is too little.

To work around that problem, one must be comfortable in the following areas of the codebase:

- Storage drivers ~"container registry::storage drivers" 
- Metadata database behavior ~"container registry::database" 
- V2 API ~"container registry::API-V2" 
- GitLab V1 API ~"container registry::API-GitLab-V1" 
- Garbage collection (online and offline) ~"container registry::garbage collection" 
- Authorization ~"container registry::authorization"

To achieve this, you should try to make relevant contributions in most of the areas mentioned above so that
you have a better understanding of the functionality. A relevant contribution may be a bug fix, a
performance improvement, a new feature, or a significant refactoring. Remember to apply the labels listed above
to your MRs to help you keep track of the changes you have made or reviewed.

Additionally, having a base understanding of the infrastructure and how the Registry is released, deployed and operated on GitLab.com or self-managed installations
should also be taken into consideration.

## Reviewer

Prior to becoming a trainee maintainer, you should first become a reviewer of the project. This should include changes
to any part of the codebase including the documentation and database schema changes.

To become a reviewer follow the steps [outlined in the handbook](https://about.gitlab.com/handbook/engineering/workflow/code-review/#reviewer).
There is no set timeline of how long you should be a reviewer before becoming a trainee maintainer, but you should
gain enough experience in the areas mentioned in the [expectations section](#expectations) of this document.

## Trainee maintainer

After being a reviewer, you may opt to become a trainee maintainer. To do so, you can open a
[container registry traineee maintainer issue](../.gitlab/issue_templates/Traineee%20Maintainer.md) in the handbook project.
Trainee maintainers should do reviews as-if they were the maintainers of the project.
They should also shadow maintainers in operational tasks, such as creating releases, making changes to the deployments on GitLab.com,
or creating/updating metrics and dashboards.

You should document and keep track of reviews you have performed as documented in the template.

## Maintainer

You are probably ready to become a maintainer when these statements feel true:

- The MRs you have reviewed consistently make it through maintainer review without significant additionally required changes
- The MRs you have created consistently make it through reviewer and maintainer review without significant required changes
- You feel comfortable working through operational tasks

If those subjective requirements are satisfied, you should follow this process in your trainee maintainer issue:

- Have well documented reviews you have made.
- Recent MRs that you have created and reviewed that you believe show your readiness.
- Get feedback from the maintainer(s) that approved the changes for MRs you reviewed or created.
- Explain why you are ready to take on the maintainer responsibility.
- Tag all maintainers of the project and ask them to vote on whether you are ready to become a maintainer.
  - If denied, keep the issue open and work with your manager for an X period before applying again.
- If approved, create an MR to add yourself as a maintainer of the project in your team entry in the handbook.
- Assign the MR to your manager.
- Ask an existing maintainer to give you the maintainer role on the project settings.
- Congratulations, you are now a maintainer of the Container Registry project!

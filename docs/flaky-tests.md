# Flaky test ownership and remediation

## Purpose

Reduce flaky tests continuously by assigning ownership each milestone, with AI-assisted investigation and remediation.

## Rotation and Ownership

- One engineer per milestone owns flaky-test remediation
- Assignment rotates round-robin and is recorded in the [rotation scoreboard](https://gitlab.com/gitlab-org/container-registry/-/work_items/2109)
- The rotation is managed automatically through the rotation scoreboard by issue `@ai-container-registry-flake-rotation-steward-gitlab-org` during milestone planning (but may also be updated manually when automation fails or if human intervention is needed to update the scoreboard)

## AI-Assisted Workflow

New and repeat flaky tests are flagged by our [triage bot](https://gitlab.com/project_13831684_bot_b6077f780c08c6981d060e7b5619faff). The triage bot identifies the flake, records its occurrence in a flake issue - [example](https://gitlab.com/gitlab-org/container-registry/-/work_items/2141) - and assigns it to [`@ai-container-registry-flake-investigator-gitlab-org`](https://gitlab.com/ai-container-registry-flake-investigator-gitlab-org) as the first point of contact. The AI agent/flow will:

- **Investigate** the flaky test failure
- **Analyze** the root cause using pipeline logs and codebase context
- **Propose** a concrete solution with implementation details - [example](https://gitlab.com/gitlab-org/container-registry/-/issues/2141#note_3092495316)
- **Implement** the proposed fix upon approval by a human (mention `@ai-container-registry-flake-fixer-gitlab-org`)

The human's role is to review the proposed solution and either approve it for implementation or apply an alternate fix based on their own investigation.

For details on where the associated AI agents and flows that orchestrate this workflow are defined, see [Agents and Flows](https://gitlab.com/components/agents-and-flows/container-registry/-/tree/main/flows?ref_type=heads).

## Guidance for Selecting a Flaky Test During Rotation

These are the rules the selection process - whether automated or manual - should follow when prioritizing what to work on.

- Start by searching for existing flaky-test issues created by automation:
  - Filter by labels: `flaky::test`, `flaky::auto`.
  - Prioritize by occurrence buckets: `flaky-occurrences::>25`, `flaky-occurrences::11-25`, `flaky-occurrences::4-10`, `flaky-occurrences::1-3`.
- Pick a candidate to work on:
  - Highest occurrence count first (sort by Weight), then most recently updated. See: [Flaky issues sorted by Weight](https://gitlab.com/gitlab-org/container-registry/-/issues?sort=weight&state=opened&label_name%5B%5D=flaky%3A%3Aauto&label_name%5B%5D=group%3A%3Acontainer%20registry&label_name%5B%5D=failure%3A%3Aflaky-test&first_page_size=20)
  - Prefer issues that are unassigned to humans already.
- Fix or mitigate:
  - Open an MR with the fix and reference the issue.
  - Add notes to the issue about root cause and remediation.
- Wrap-up:
  - Close the issue once fixed; automation will reopen if it flakes again.
  - If not resolved within the milestone, document findings and hand off to the next rotation during the next milestone planning.

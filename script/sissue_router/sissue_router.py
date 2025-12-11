#!/usr/bin/env python3
"""
GitLab Issue Fetcher

Fetches issues from GitLab and displays them with title, labels, status, and assignment events.
"""

import random
import re
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import click
import gitlab

# Constants
GITLAB_PROJECT = "gitlab-com/request-for-help"
FILTER_LABELS = ["Help group::container registry"]
ISSUE_STATE = "all"
SORT_ORDER = "asc"
SORT_BY = "created_at"

team_members = ["adie.po", "hswimelar", "vespian_gl", "jdrpereira", "suleimiahmed"]

# Automation tag pattern
TAG_VALUE_PATTERN = re.compile(r'^[a-zA-Z0-9_\-:]+$')
AUTOMATION_TAG_PATTERN = re.compile(
    r'^SISSUE_AUTOMATION_TAGS:\s*(.+)$', re.MULTILINE)


def format_date(date_str: str) -> str:
    """Format ISO date string to readable format."""
    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_assignment_events(issue) -> List[str]:
    """Get assignment and unassignment events for an issue."""
    events = []

    # Check issue notes for assignment changes
    notes = issue.notes.list(get_all=True)
    for note in notes:
        if note.system:
            body_lower = note.body.lower()
            if 'assigned to' not in body_lower and 'unassigned' not in body_lower:
                continue

            event_info = f"{note.body} by {note.author['username']} on {note.created_at}"
            events.append(event_info)

    return events


def parse_automation_tags(tag_string: str) -> Tuple[Optional[str], List[str]]:
    """
    Parse automation tags from a comment line.

    Returns:
        Tuple of (assigned_to_user, list_of_errors)
    """
    tags = [tag.strip() for tag in tag_string.split(',')]
    assigned_to = None
    errors = []

    for tag in tags:
        if not TAG_VALUE_PATTERN.match(tag):
            errors.append(f"Invalid tag format: '{tag}'")
            continue

        if tag.startswith('ASSIGNED_TO:'):
            if assigned_to is not None:
                errors.append(f"Duplicate ASSIGNED_TO tag found: '{tag}'")
                continue
            assigned_to = tag.split(':', 1)[1]
        else:
            errors.append(f"Unrecognized tag: '{tag}'")

    return assigned_to, errors


def check_automation_status(issue) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Check if issue has ever been processed by the automation bot.

    Returns:
        Tuple of (has_been_processed, assigned_user, comment_date)
    """
    notes = issue.notes.list(get_all=True)
    for note in notes:
        matches = AUTOMATION_TAG_PATTERN.findall(note.body)
        if not matches:
            continue

        tag_string = matches[0]
        assigned_to, errors = parse_automation_tags(tag_string)

        if errors:
            click.echo(
                f"  WARNING unable to parse automation status for issue #{issue.iid}:", err=True)
            for error in errors:
                click.echo(f"    - {error}", err=True)

        if assigned_to:
            return True, assigned_to, note.created_at

    return False, None, None


def display_issue(issue, idx: int, verbose: bool):
    """Display issue information with optional verbose details."""
    if verbose:
        click.echo(f"\nIssue #{issue.iid}")
        click.echo(f"  - Title: {issue.title}")
        click.echo(f"  - Created: {format_date(issue.created_at)}")
        click.echo(f"  - Updated: {format_date(issue.updated_at)}")
        click.echo(f"  - Status: {issue.state}")
        click.echo(f"  - URL: {issue.web_url}")

        # Assignees
        if hasattr(issue, 'assignees') and issue.assignees:
            assignee_names = [
                f"{a['name']} (@{a['username']})" for a in issue.assignees]
            click.echo(f"  - Assignees: {', '.join(assignee_names)}")
        else:
            click.echo("  - Assignees: None")

        # Assignment events
        click.echo("  - Assignment Events:")
        events = get_assignment_events(issue)
        if events:
            for event in events:
                click.echo(f"    - {event}")
        else:
            click.echo("    - No assignment events found")
    else:
        # Simple format: just issue number, title, and link
        click.echo(f"\nIssue #{issue.iid}")
        click.echo(f"  - Title: {issue.title}")
        click.echo(f"  - URL: {issue.web_url}")


@click.command()
@click.option(
    "--gl-token",
    envvar="SISSUE_ROUTER_GL_TOKEN",
    help="GitLab API token",
    required=True
)
@click.option(
    "--period",
    type=int,
    default=30,
    help="Number of days to look back for issues (default: 30)"
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose output with detailed issue information"
)
def fetch_issues(gl_token: str, period: int, verbose: bool):
    """
    Fetch issues from GitLab based on specified criteria.

    Example usage:
        python script.py --gl-token YOUR_TOKEN --period 7

    Or with environment variable:
        export SISSUE_ROUTER_GL_TOKEN=YOUR_TOKEN
        python script.py --period 7

    For detailed information:
        python script.py --period 7 --verbose
    """
    gl = gitlab.Gitlab('https://gitlab.com', private_token=gl_token)

    try:
        proj = gl.projects.get(GITLAB_PROJECT)
        click.echo(f"Fetching issues from project: {proj.name}")
        click.echo(f"Looking back {period} days")
        if verbose:
            click.echo(f"Labels: {', '.join(FILTER_LABELS)}")
            click.echo(f"State: {ISSUE_STATE}")
            click.echo("Filter: Both assigned and unassigned issues")
        click.echo("=" * 80)

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=period)

        created_after = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        if verbose:
            click.echo(f"Filtering issues created after: {created_after}")

        issues = proj.issues.list(
            state=ISSUE_STATE,
            labels=FILTER_LABELS,
            created_after=created_after,
            order_by=SORT_BY,
            sort=SORT_ORDER,
            get_all=True
        )

        click.echo(f"\nFound {len(issues)} issues\n")

        unassigned_issues = []
        assign_counts = {k: 0 for k in team_members}

        for idx, issue in enumerate(issues, 1):
            display_issue(issue, idx, verbose)

            is_assigned = hasattr(issue, 'assignees') and issue.assignees and len(
                issue.assignees) > 0

            if not is_assigned:
                if issue.state == "closed":
                    click.echo(
                        "  - is closed, no need to schedule/assign it.")
                    continue

                unassigned_issues.append(issue)
                continue

            assigned_to_team = False
            for a in [x['username'].lstrip("@") for x in issue.assignees]:
                if a in assign_counts:
                    assigned_to_team = True
                    assign_counts[a] += 1

            if assigned_to_team:
                continue

            # NOTE(prozlach): Here is the tricky part - issue is assigned, but
            # not to one of our team members. We need to investigate if it was
            # triaged by us and if so - count it as assigned issue, as this is
            # also work done on the team member's part.

            # Check automation status
            has_been_processed, assigned_user, comment_date = check_automation_status(
                issue)

            if has_been_processed:
                assign_counts[assigned_user] += 1
                click.echo(
                    f"  - has been assigned by sissue bot on {comment_date}")
                continue

            # NOTE(prozlach): issue has been manually assigned to somebody
            # outside of our team, we should just ignore it.
            click.echo(
                "  - has been assigned manually to somebody outside of our team, ignoring."
            )

        # Summary
        click.echo(f"\n{'=' * 80}")
        click.echo(f"Total issues found: {len(issues)}")
        click.echo(
            f"Assign counts:\n  {assign_counts}"
        )

        if not unassigned_issues:
            click.echo("There are no issues that require to be assigned. Bye!")
            return

        click.echo(
            "Issues that need to be assigned:")
        for i in unassigned_issues:
            click.echo(f"  - '{i.title}': {i.web_url}")

        # Assign the issues themselves
        for ua in unassigned_issues:
            # Find username(s) with the least amount of issues assigned
            min_count = min(assign_counts.values())
            candidates = [user for user,
                          count in assign_counts.items() if count == min_count]

            # Pick one randomly if there are multiple candidates
            selected_user = random.choice(candidates)

            click.echo(f"\nAssigning issue #{ua.iid} to @{selected_user}")

            try:
                # Assign the issue (GitLab API expects user ID, not username)
                # First, we need to find the user ID from the username
                gl_users = gl.users.list(username=selected_user, get_all=False)

                if not gl_users:
                    click.echo(
                        f"  ERROR: Could not find user with username '{selected_user}'", err=True)
                    continue

                user_id = gl_users[0].id

                # Assign the issue
                ua.assignee_ids = [user_id]
                ua.save()

                # Bump the assign count
                assign_counts[selected_user] += 1

                # Add comment with automation tags
                comment_body = \
                    f"""Issue has been assigned by sissue bot to @{selected_user}

<details>
<summary>Automation tags</summary>

SISSUE_AUTOMATION_TAGS: ASSIGNED_TO:{selected_user}
</details>"""

                ua.notes.create({'body': comment_body})

                click.echo(f"  ✓ Successfully assigned to @{selected_user}")
                click.echo(f"  ✓ Comment added with automation tags")

            except gitlab.exceptions.GitlabUpdateError as e:
                click.echo(
                    f"  ERROR: Failed to assign issue #{ua.iid}: {str(e)}", err=True)
            except gitlab.exceptions.GitlabCreateError as e:
                click.echo(
                    f"  ERROR: Failed to add comment to issue #{ua.iid}: {str(e)}", err=True)

    except gitlab.exceptions.GitlabAuthenticationError:
        click.echo(
            "Error: Authentication failed. Please check your GitLab token.", err=True)
        raise click.Abort()
    except gitlab.exceptions.GitlabGetError as e:
        click.echo(
            f"Error: Could not fetch project '{GITLAB_PROJECT}'. {str(e)}", err=True)
        raise click.Abort()


if __name__ == "__main__":
    try:
        fetch_issues()
    except Exception as e:
        click.echo(
            "  ERROR: Exception occurred: ", err=True)
        click.echo(f"    Exception type: {type(e).__name__}", err=True)
        click.echo(f"    Exception message: {str(e)}", err=True)
        click.echo(
            f"    File: {traceback.extract_tb(e.__traceback__)[-1].filename}", err=True)
        click.echo(
            f"    Line: {traceback.extract_tb(e.__traceback__)[-1].lineno}", err=True)
        click.echo(
            f"    Function: {traceback.extract_tb(e.__traceback__)[-1].name}", err=True)
        click.echo("  Stacktrace:", err=True)
        for line in traceback.format_exception(type(e), e, e.__traceback__):
            for subline in line.rstrip().split('\n'):
                click.echo(f"    {subline}", err=True)

#!/usr/bin/env python3

import base64
import hashlib
import os
import re
import shelve
import traceback
from datetime import datetime, timedelta

import click
import gitlab
import psycopg
import yaml
from dateutil.parser import parse

# Constants
KIND_ID_OVERALL = 1
KIND_TYPE_ID_OVERALL = 1
KIND_TYPE_ID_VARIANT_GROUP = 2
KIND_TYPE_ID_VARIANT = 3
KIND_TYPE_ID_TEST = 4
# NOTE(prozlach): pg_partman retention is set to 9 weeks = 63 days. Grafana
# starts acting up with too many datapoints returned from PG so bumping this
# requires some more work on both the DB settings/queries and the Grafana settings.
# It should be enough though, as CI tasks older than 63 days aren't that usefull anyway.
DEFAULT_PERIOD_DAYS = 63

CR_PROJECT_ID = 13831684


def parse_gitlab_ci_allowfail_patterns(gitlab_ci_path):
    """
    Parse .gitlab-ci.yml file and extract allowfail regexp patterns.

    Returns a list of compiled regular expression objects from comments
    in the format: #ci-tool-allowfail regexp
    """
    allowfail_patterns = []

    try:
        with open(gitlab_ci_path, 'r') as f:
            for line in f:
                # Look for comments with the specific format
                match = re.search(
                    r'#\s*ci-tool-allowfail\s+(.+)$', line.strip())
                if match:
                    pattern = match.group(1).strip()
                    try:
                        compiled_pattern = re.compile(pattern)
                        allowfail_patterns.append(compiled_pattern)
                        print(f"Added allowfail pattern: {pattern}")
                    except re.error as e:
                        print(
                            f"Warning: Invalid regexp pattern '{pattern}': {e}")
    except FileNotFoundError:
        print(f"Error: GitLab CI file not found at {gitlab_ci_path}")
        raise
    except Exception as e:
        print(f"Error reading GitLab CI file: {e}")
        raise

    return allowfail_patterns


def process_test_report(cursor, test_report, mapping, date, datetime, pipeline_id, allowfail_patterns=None):
    success_tuples = []
    fail_tuples = []
    time_tuples = []
    overall_success = True

    if allowfail_patterns is None:
        allowfail_patterns = []

    for test_suite in test_report.test_suites:
        # Check if this test suite matches any allowfail pattern
        is_allowfail = any(pattern.search(
            test_suite["name"]) for pattern in allowfail_patterns)

        variant_group_id = ""
        variant_id = ""
        if ': [' in test_suite["name"]:
            variant_group_name = ":".join(test_suite["name"].split(":")[:-1])
            variant_group_id = mapping[KIND_TYPE_ID_VARIANT_GROUP][variant_group_name]
            variant_id = mapping[KIND_TYPE_ID_VARIANT][test_suite["name"]]

            time_tuples.append(
                (datetime, variant_id, test_suite["total_time"], pipeline_id))
        else:
            variant_group_id = mapping[KIND_TYPE_ID_VARIANT_GROUP][test_suite["name"]]

            time_tuples.append((datetime, variant_group_id,
                               test_suite["total_time"], pipeline_id))

        if test_suite["failed_count"] == 0:
            success_tuples.append((date, variant_group_id, 1))
            if variant_id:
                success_tuples.append((date, variant_id, 1))
        else:
            if not is_allowfail:
                fail_tuples.append((date, variant_group_id, 1, 1, pipeline_id))
                if variant_id:
                    fail_tuples.append((date, variant_id, 1, 1, pipeline_id))
                # Already handled overall_success above with allowfail check
            else:
                print(
                    f"Variant Group `{variant_group_name}` allowed to fail")
                if variant_id:
                    print(
                        f"Variant `{test_suite['name']} allowed to fail")

        for test_case in test_suite["test_cases"]:
            tc_name_id = mapping[KIND_TYPE_ID_TEST][test_case["name"]]
            time_tuples.append(
                (datetime, tc_name_id, test_case["execution_time"], pipeline_id))

            if test_case["status"] == "failed":
                fail_tuples.append((date, tc_name_id, 1, 1, pipeline_id))
                # Don't update overall_success for individual test cases if parent suite is allowfail
                if not is_allowfail:
                    overall_success = False
                else:
                    print(
                        f"Testcase {test_case["name"]} allowed to fail")
            elif test_case["status"] == "success":
                success_tuples.append((date, tc_name_id, 1))

    if overall_success:
        success_tuples.append((date, KIND_ID_OVERALL, 1))
    else:
        fail_tuples.append((date, KIND_ID_OVERALL, 1, 1, pipeline_id))

    time_tuples.append(
        (datetime, KIND_ID_OVERALL, test_report.total_time, pipeline_id))

    return success_tuples, fail_tuples, time_tuples


def ensure_mappings(cursor, test_report):
    mapping = {i: {} for i in [
        KIND_TYPE_ID_OVERALL, KIND_TYPE_ID_VARIANT_GROUP, KIND_TYPE_ID_VARIANT, KIND_TYPE_ID_TEST]}
    last_id = -1
    missing_mappings = []

    cursor.execute("SELECT id, type_id, text FROM kind_ids ORDER BY id")
    for row in cursor:
        mapping[row[1]][row[2]] = row[0]
        last_id = row[0]

    for test_suite in test_report.test_suites:
        variant_group_name = ""
        variant_name = ""
        if ': [' in test_suite["name"]:
            variant_group_name = ":".join(test_suite["name"].split(":")[:-1])
            variant_name = test_suite["name"]
        else:
            variant_group_name = test_suite["name"]

        if variant_group_name not in mapping[KIND_TYPE_ID_VARIANT_GROUP]:
            missing_mappings.append(
                (KIND_TYPE_ID_VARIANT_GROUP, variant_group_name))
            mapping[KIND_TYPE_ID_VARIANT_GROUP][variant_group_name] = -1

        if variant_name and variant_name not in mapping[KIND_TYPE_ID_VARIANT]:
            missing_mappings.append((KIND_TYPE_ID_VARIANT, test_suite["name"]))
            mapping[KIND_TYPE_ID_VARIANT][test_suite["name"]] = -1

        for test_case in test_suite["test_cases"]:
            tc_name = test_case["name"]
            if tc_name not in mapping[KIND_TYPE_ID_TEST]:
                missing_mappings.append((KIND_TYPE_ID_TEST, tc_name))
                mapping[KIND_TYPE_ID_TEST][tc_name] = -1

    cursor.executemany(
        "INSERT INTO kind_ids (type_id, text) VALUES (%s, %s)", missing_mappings)

    # Re-fetch mapping now that the database has assigned the IDs
    cursor.execute(
        "SELECT id, type_id, text FROM kind_ids WHERE id > %s ORDER BY id", (last_id,))
    for row in cursor:
        mapping[row[1]][row[2]] = row[0]

    return mapping


def process_pipeline(cursor, pipeline, use_cache, write_jsons, allowfail_patterns):
    test_report = None

    if use_cache:
        with shelve.open("pipelines.shelve") as pipelines:
            if str(pipeline.id) not in pipelines:
                print(
                    f"Fetching pipeline {pipeline.id} and storing it in the local cache")
                test_report = pipeline.test_report.get()
                pipelines[str(pipeline.id)] = {"test_report": test_report}
            else:
                test_report = pipelines[str(pipeline.id)]["test_report"]
                print(f"Pipeline {pipeline.id} found in cache")
    else:
        print(f"Fetching pipeline {pipeline.id}")
        test_report = pipeline.test_report.get()

    datetime_str = pipeline.created_at
    date_str = parse(datetime_str).strftime('%Y-%m-%d')

    if write_jsons:
        with open(f"./pipeline {pipeline.id}.{date_str}.test_report.json", 'w') as file:
            # Pretty-format the JSON test report and write it to the file in case we need to do some grepping later on
            file.write(test_report.to_json())

    # Mark pipeline as processed:
    cursor.execute(
        "INSERT INTO public.pipelines (id, web_url) VALUES (%s, %s)",
        (pipeline.id, pipeline.web_url)
    )

    mapping = ensure_mappings(cursor, test_report)
    success_tuples, fail_tuples, time_tuples = process_test_report(
        cursor, test_report, mapping, date_str, datetime_str, pipeline.id, allowfail_patterns)

    cursor.executemany(
        """INSERT INTO public.success_ratio (date, kind_id, total_runs)
                VALUES (%s, %s, %s)
            ON CONFLICT (date, kind_id) DO UPDATE
            SET
                total_runs = public.success_ratio.total_runs + EXCLUDED.total_runs
        """,
        success_tuples
    )

    cursor.executemany(
        """INSERT INTO public.success_ratio (date, kind_id, total_runs, failed_runs, pipeline_ids)
                VALUES (%s, %s, %s, %s, ARRAY[%s])
            ON CONFLICT (date, kind_id) DO UPDATE
            SET
                total_runs = public.success_ratio.total_runs + EXCLUDED.total_runs,
                failed_runs = public.success_ratio.failed_runs + EXCLUDED.failed_runs,
                pipeline_ids = (
                    SELECT ARRAY(
                        SELECT DISTINCT unnest(
                            public.success_ratio.pipeline_ids || EXCLUDED.pipeline_ids
                        )
                    )
                )
        """,
        fail_tuples
    )

    cursor.executemany(
        """INSERT INTO public.execution_time (datetime, kind_id, duration_seconds, pipeline_id)
             VALUES (%s, %s, ARRAY[%s], %s)
           ON CONFLICT (datetime, kind_id, pipeline_id) DO UPDATE 
            SET
              duration_seconds = execution_time.duration_seconds || EXCLUDED.duration_seconds
        """,
        time_tuples
    )


@click.command()
@click.option('--use-cache', is_flag=True)
@click.option('--write-jsons', is_flag=True)
@click.option("--db-host", envvar="CI_TOOL_DB_HOST", help="PostgreSQL database host", required=True)
@click.option("--db-name", envvar="CI_TOOL_DB_NAME", help="PostgreSQL database name", required=True)
@click.option("--db-user", envvar="CI_TOOL_DB_USER", help="PostgreSQL database user", required=True)
@click.option("--db-pass", envvar="CI_TOOL_DB_PASS", help="PostgreSQL database password", required=True)
@click.option("--db-port", envvar="CI_TOOL_DB_PORT", default=5432, type=click.IntRange(min=1024, max=65536), help="PostgreSQL database port", show_default=True)
@click.option("--gl-token", envvar="CI_TOOL_GL_TOKEN", help="GitLab API token", required=True)
@click.option("--period", type=click.IntRange(min=1, max=DEFAULT_PERIOD_DAYS), default=DEFAULT_PERIOD_DAYS, help="Number of days to look back when quering pipeline data from GitLab", show_default=True)
@click.option("--gitlab-ci-file", type=click.Path(exists=True, readable=True), help="Path to .gitlab-ci.yml file", required=True)
def main(use_cache, write_jsons, db_host, db_name, db_user, db_pass, db_port, gl_token, period, gitlab_ci_file):
    since = datetime.now() - timedelta(days=period)

    # Parse allowfail patterns from GitLab CI file
    print(f"Parsing allowfail patterns from {gitlab_ci_file}")
    allowfail_patterns = parse_gitlab_ci_allowfail_patterns(gitlab_ci_file)
    print(f"Found {len(allowfail_patterns)} allowfail patterns")

    print("Connecting to GitLab")
    gl = gitlab.Gitlab(private_token=gl_token)
    project = gl.projects.get(CR_PROJECT_ID)
    pipelines_iterator = project.pipelines.list(
        iterator=True, ref="master", order_by="id", updated_after=since.isoformat())
    print(f"{pipelines_iterator.total} pipelines found for the given period")

    print("Connecting to PostgreSQL")
    conn_str = f"dbname={db_name} host={db_host} user={db_user} password={db_pass} port={db_port} sslmode=require"
    conn = psycopg.connect(conn_str)
    cursor = conn.cursor()

    try:
        for pipeline in pipelines_iterator:
            cursor.execute(
                "SELECT 1 FROM public.pipelines WHERE id = %s", (pipeline.id,))
            if cursor.fetchone():
                print(
                    f"Pipeline with ID {pipeline.id} already in the database")
                continue

            print(f"Processing pipeline ID {pipeline.id}")
            process_pipeline(cursor, pipeline, use_cache,
                             write_jsons, allowfail_patterns)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

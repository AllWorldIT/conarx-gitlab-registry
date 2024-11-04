-- NOTE(prozlach): By default, the pg_cron background worker expects its metadata tables to be
-- created in the "postgres" database. In theory we can configure this by
-- setting the cron.database_name configuration parameter in postgresql.conf
-- but in practice it seems even clunkier, hence two sets of migrations
--
-- NOTE(prozlach): on AWS RDS you also need to set up custom parameter group.
-- See: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_pg_cron.html
CREATE EXTENSION pg_cron;

SELECT cron.schedule('cron maintenance', '@daily', $$DELETE FROM cron.job_run_details WHERE end_time < now() - interval '28 days'$$);

-- pre-creates partitions, can be run frequently/is a lightweight task
SELECT cron.schedule_in_database('partman partitions maintenance', '@hourly', $$SELECT partman.run_maintenance()$$, 'ci_analitics');

-- moves data from default partition to the correct time-ranged partition,
-- normaly should be a no-op unless there was a big import so we can run it
-- frequently too
SELECT cron.schedule_in_database('partman default table maintenance - success_ratio', '@hourly', $$CALL partman.partition_data_proc('public.success_ratio')$$, 'ci_analitics');
SELECT cron.schedule_in_database('partman default table maintenance - execution_time', '@hourly', $$CALL partman.partition_data_proc('public.execution_time')$$, 'ci_analitics');

-- purge old entries from kind_ids and pipelines tables that CASCADE statement
-- does not take care of
SELECT cron.schedule_in_database('kind_ids table maintenance', '@weekly', $$DELETE                             
FROM kind_ids k
WHERE NOT EXISTS (
    SELECT 1
    FROM success_ratio e
    WHERE e.kind_id = k.id
)$$, 'ci_analitics');
SELECT cron.schedule_in_database('pipelines table maintenance', '@weekly', $$WITH oldest_row AS (
    SELECT pipeline_ids
    FROM public.success_ratio
    ORDER BY date ASC
    LIMIT 1
),
min_pipeline_id AS (
    SELECT MIN(pipeline_id) AS min_id
    FROM oldest_row, LATERAL unnest(pipeline_ids) AS pipeline_id
)
DELETE FROM public.pipelines
WHERE id < (SELECT min_id FROM min_pipeline_id)$$, 'ci_analitics');

-- housekeeping
SELECT cron.schedule_in_database('vacuum', '@daily', 'VACUUM ANALYZE', 'ci_analitics');

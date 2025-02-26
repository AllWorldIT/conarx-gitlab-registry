SELECT cron.unschedule('cron maintenance');
SELECT cron.unschedule('partman partitions maintenance');
SELECT cron.unschedule('partman default table maintenance - success_ratio');
SELECT cron.unschedule('partman default table maintenance - execution_time');
SELECT cron.unschedule('kind_ids table maintenance');
SELECT cron.unschedule('pipelines table maintenance');
SELECT cron.unschedule('vacuum');

DROP EXTENSION pg_cron CASCADE;

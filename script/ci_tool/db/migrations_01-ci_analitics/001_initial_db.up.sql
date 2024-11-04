-- create users:
create user ci_tool_writer;
GRANT CONNECT ON DATABASE ci_analitics to ci_tool_writer;
GRANT USAGE ON SCHEMA public TO ci_tool_writer;
--NOTE(prozlach): we want to give access to all future partitions that are
--going to be created by partman
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public GRANT SELECT,INSERT,UPDATE ON TABLES TO ci_tool_writer;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO ci_tool_writer;

create user ci_tool_reader;
GRANT CONNECT ON DATABASE ci_analitics to ci_tool_reader;
GRANT USAGE ON SCHEMA public TO ci_tool_reader;
--NOTE(prozlach): we want to give access to all future partitions that are
--going to be created by partman
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public GRANT SELECT ON TABLES TO ci_tool_reader;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public GRANT SELECT ON SEQUENCES TO ci_tool_reader;

-- partman extension needs to be created in the target db
create schema partman;
CREATE EXTENSION pg_partman schema partman;

-- create tables
-- `kind_type_ids` describes the type of the particular type
CREATE TABLE public.kind_type_ids (
    id SERIAL PRIMARY KEY,
    description TEXT NOT NULL
);
INSERT INTO public.kind_type_ids(description) VALUES ('overall'), ('variant_group'), ('variant'), ('test');

-- `kind_ids` describes the particular entry - name of the test, variant group, variant,
-- etc...
CREATE TABLE public.kind_ids (
    id SERIAL PRIMARY KEY,
    type_id INT REFERENCES kind_type_ids(id) ON DELETE CASCADE,
    text TEXT NOT NULL
);
insert into public.kind_ids(type_id, text) values (1, 'overall');

-- `pipelines` table describes a link from pipeline_id to its web url
CREATE TABLE public.pipelines (
    id INT PRIMARY KEY,
    web_url TEXT NOT NULL
);

-- `success_ratio` contains information on failed tests, variant groups,
-- variants, and overall ci runs
CREATE TABLE public.success_ratio (
    date DATE NOT NULL,
    kind_id INT REFERENCES kind_ids(id) ON DELETE CASCADE,
    total_runs INT NOT NULL,
    failed_runs INT DEFAULT 0 NOT NULL,
    pipeline_ids INT[] DEFAULT ARRAY[]::INT[] NOT NULL,
    PRIMARY KEY (date, kind_id)
) PARTITION BY RANGE (date);

SELECT partman.create_parent(
  'public.success_ratio',
  p_control := 'date',
  p_type := 'range',
  p_interval := '1 week',
  p_premake := 2,
  p_start_partition := to_char(date_trunc('week',CURRENT_TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS'),
  p_automatic_maintenance := 'on'
);

UPDATE partman.part_config SET
    retention = '9 weeks',
    retention_keep_table=false
  WHERE parent_table = 'public.success_ratio';

-- `execution_time` contains information on execution time of tests, variant
-- groups, variants, and overall ci runs
CREATE TABLE public.execution_time (
    datetime TIMESTAMP WITH TIME ZONE,
    kind_id INT REFERENCES kind_ids(id) ON DELETE CASCADE,
    duration_seconds NUMERIC(10, 4)[] DEFAULT '{}' NOT NULL,
    pipeline_id INT NOT NULL,
    PRIMARY KEY (datetime, kind_id, pipeline_id)
) PARTITION BY RANGE (datetime);

SELECT partman.create_parent(
  'public.execution_time',
  p_control := 'datetime',
  p_type := 'range',
  p_interval := '1 week',
  p_premake := 2,
  p_start_partition := to_char(date_trunc('week',CURRENT_TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS'),
  p_automatic_maintenance := 'on'
);

UPDATE partman.part_config SET
    retention = '9 weeks',
    retention_keep_table=false
  WHERE parent_table = 'public.execution_time';

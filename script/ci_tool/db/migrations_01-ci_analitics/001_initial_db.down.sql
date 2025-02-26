-- reverse order so that we keep foreign key constraints right
DELETE FROM partman.part_config WHERE parent_table = 'public.execution_time';
DROP TABLE IF EXISTS public.execution_time;
DELETE FROM partman.part_config WHERE parent_table = 'public.success_ratio';
DROP TABLE IF EXISTS public.success_ratio;
DROP TABLE IF EXISTS public.pipelines;
DROP TABLE IF EXISTS public.kind_ids;
DROP TABLE IF EXISTS public.kind_type_ids;

DROP EXTENSION pg_partman CASCADE;
DROP SCHEMA partman CASCADE;

REVOKE CONNECT ON DATABASE ci_analitics FROM ci_tool_writer;
REVOKE USAGE ON SCHEMA public FROM ci_tool_writer;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ci_tool_writer;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM ci_tool_writer;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM ci_tool_writer;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public REVOKE SELECT,INSERT,UPDATE ON TABLES FROM ci_tool_writer;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public REVOKE ALL PRIVILEGES ON SEQUENCES FROM ci_tool_writer;
drop user ci_tool_writer;

REVOKE CONNECT ON DATABASE ci_analitics FROM ci_tool_reader;
REVOKE USAGE ON SCHEMA public FROM ci_tool_reader;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ci_tool_reader;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM ci_tool_reader;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM ci_tool_reader;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public REVOKE SELECT ON TABLES FROM ci_tool_reader;
ALTER DEFAULT PRIVILEGES FOR USER psqladm IN SCHEMA public REVOKE SELECT ON SEQUENCES FROM ci_tool_reader;
drop user ci_tool_reader;

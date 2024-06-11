package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240604074846_create_batched_background_migration_jobs_table",
			Up: []string{
				`CREATE TABLE batched_background_migration_jobs (
					id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
					created_at timestamp WITH time zone NOT NULL DEFAULT now(),
					updated_at timestamp WITH time zone,
					started_at timestamp WITH time zone,
					finished_at timestamp WITH time zone,
					batched_background_migration_id bigint NOT NULL,
					min_value bigint NOT NULL,
					max_value bigint NOT NULL,
					status smallint DEFAULT 1 NOT NULL,
					failure_error_code smallint,
					attempts smallint DEFAULT 0 NOT NULL,
					CONSTRAINT pk_batched_background_migrations_job PRIMARY KEY (id),
					CONSTRAINT fk_batched_background_migration_jobs_bbm_id_bbms FOREIGN KEY (batched_background_migration_id) REFERENCES batched_background_migrations (id) ON DELETE CASCADE
				)`},
			Down: []string{"DROP TABLE IF EXISTS batched_background_migration_jobs CASCADE"},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}

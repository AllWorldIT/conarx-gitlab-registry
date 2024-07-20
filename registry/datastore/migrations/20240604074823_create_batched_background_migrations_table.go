package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240604074823_create_batched_background_migrations_table",
			Up: []string{
				`CREATE TABLE batched_background_migrations (
					id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
					name text NOT NULL,
					created_at timestamp WITH time zone NOT NULL DEFAULT now(),
					updated_at timestamp WITH time zone,
					min_value bigint DEFAULT 1 NOT NULL,
					max_value bigint NOT NULL,
					batch_size integer NOT NULL,
					status smallint DEFAULT 0 NOT NULL,
					job_signature_name text NOT NULL,
					table_name text NOT NULL,
					column_name text NOT NULL,
					CONSTRAINT pk_batched_background_migrations PRIMARY KEY (id),
					CONSTRAINT unique_batched_background_migrations_name UNIQUE (name)
				)`,
			},
			Down: []string{"DROP TABLE IF EXISTS batched_background_migrations CASCADE"},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}

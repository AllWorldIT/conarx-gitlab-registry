package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503153320_create_gc_blob_review_queue_table",
			Up: []string{
				`CREATE TABLE IF NOT EXISTS gc_blob_review_queue (
					review_after timestamp with time zone NOT NULL DEFAULT now() + interval '1 day',
					review_count integer NOT NULL DEFAULT 0,
					digest bytea NOT NULL,
					CONSTRAINT pk_gc_blob_review_queue PRIMARY KEY (digest)
				)`,
				"CREATE INDEX IF NOT EXISTS index_gc_blob_review_queue_on_review_after ON gc_blob_review_queue USING btree (review_after)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_gc_blob_review_queue_on_review_after CASCADE",
				"DROP TABLE IF EXISTS gc_blob_review_queue CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}

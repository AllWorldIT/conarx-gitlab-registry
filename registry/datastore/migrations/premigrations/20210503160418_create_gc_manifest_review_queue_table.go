package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503160418_create_gc_manifest_review_queue_table",
			Up: []string{
				`CREATE TABLE IF NOT EXISTS gc_manifest_review_queue (
					top_level_namespace_id bigint NOT NULL,
					repository_id bigint NOT NULL,
					manifest_id bigint NOT NULL,
					review_after timestamp WITH time zone NOT NULL DEFAULT now() + interval '1 day',
					review_count integer NOT NULL DEFAULT 0,
					CONSTRAINT pk_gc_manifest_review_queue PRIMARY KEY (top_level_namespace_id, repository_id, manifest_id),
					CONSTRAINT fk_gc_manifest_review_queue_tp_lvl_nspc_id_rp_id_mfst_id_mnfsts FOREIGN KEY (top_level_namespace_id, repository_id, manifest_id) REFERENCES manifests (top_level_namespace_id, repository_id, id) ON DELETE CASCADE
				)`,
				"CREATE INDEX IF NOT EXISTS index_gc_manifest_review_queue_on_review_after ON gc_manifest_review_queue USING btree (review_after)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_gc_manifest_review_queue_on_review_after CASCADE",
				"DROP TABLE IF EXISTS gc_manifest_review_queue CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}

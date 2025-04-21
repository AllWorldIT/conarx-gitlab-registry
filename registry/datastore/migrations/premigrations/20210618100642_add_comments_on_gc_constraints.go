package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210618100642_add_comments_on_gc_constraints",
			Up: []string{
				"COMMENT ON CONSTRAINT unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ON manifests IS 'Unique constraint required to optimize the cascade on delete from manifests to gc_blobs_configurations'",
				"COMMENT ON CONSTRAINT unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ON layers IS 'Unique constraint required to optimize the cascade on delete from manifests to gc_blobs_layers, through layers'",
			},
			Down: []string{
				"COMMENT ON CONSTRAINT unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ON manifests IS NULL",
				"COMMENT ON CONSTRAINT unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ON layers IS NULL",
			},
		},
	}

	migrations.AppendPreMigration(m)
}

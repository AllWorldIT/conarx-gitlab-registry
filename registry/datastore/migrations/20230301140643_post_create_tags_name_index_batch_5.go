//go:build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230301140643_post_create_tags_name_index_batch_5",
			Up: []string{
				"CREATE INDEX index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ON public.tags USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}

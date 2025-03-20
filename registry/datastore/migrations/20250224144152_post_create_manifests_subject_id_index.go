package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20250224144152_post_create_manifests_subject_id_index",
			Up: []string{
				"CREATE INDEX index_manifests_on_ns_id_and_repo_id_and_subject_id ON public.manifests USING btree (top_level_namespace_id, repository_id, subject_id)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_manifests_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}

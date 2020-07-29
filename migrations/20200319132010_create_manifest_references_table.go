package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20200319132010_create_manifest_references_table",
		Up: []string{
			`CREATE TABLE IF NOT EXISTS manifest_references (
                id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                parent_id bigint NOT NULL,
                child_id bigint NOT NULL,
                created_at timestamp WITH time zone NOT NULL DEFAULT now(),
                CONSTRAINT pk_manifest_references PRIMARY KEY (id),
                CONSTRAINT fk_manifest_references_parent_id_manifests FOREIGN KEY (parent_id) REFERENCES manifests (id) ON DELETE CASCADE,
                CONSTRAINT fk_manifest_references_child_id_manifests FOREIGN KEY (child_id) REFERENCES manifests (id) ON DELETE CASCADE,
                CONSTRAINT uq_manifest_references_parent_id_child_id UNIQUE (parent_id, child_id),
				CONSTRAINT ck_manifest_references_parent_id_child_id_differ CHECK ((parent_id <> child_id))
            )`,
			"CREATE INDEX IF NOT EXISTS ix_manifest_references_parent_id ON manifest_references (parent_id)",
			"CREATE INDEX IF NOT EXISTS ix_manifest_references_child_id ON manifest_references (child_id)",
		},
		Down: []string{
			"DROP INDEX IF EXISTS ix_manifest_references_child_id CASCADE",
			"DROP INDEX IF EXISTS ix_manifest_references_parent_id CASCADE",
			"DROP TABLE IF EXISTS manifest_references CASCADE",
		},
	}

	allMigrations = append(allMigrations, m)
}
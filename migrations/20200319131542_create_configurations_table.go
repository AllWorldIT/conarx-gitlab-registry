package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20200319131542_create_configurations_table",
		Up: []string{
			`CREATE TABLE IF NOT EXISTS configurations (
                id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                blob_id bigint NOT NULL,
                created_at timestamp WITH time zone NOT NULL DEFAULT now(),
                payload bytea NOT NULL,
                CONSTRAINT pk_configurations PRIMARY KEY (id),
                CONSTRAINT fk_configurations_blob_id_blobs FOREIGN KEY (blob_id) REFERENCES blobs (id) ON DELETE CASCADE,
                CONSTRAINT uq_configurations_blob_id UNIQUE (blob_id)
            )`,
			"CREATE INDEX IF NOT EXISTS ix_configurations_blob_id ON configurations (blob_id)",
		},
		Down: []string{
			"DROP INDEX IF EXISTS ix_configurations_blob_id CASCADE",
			"DROP TABLE IF EXISTS configurations CASCADE",
		},
	}

	allMigrations = append(allMigrations, m)
}
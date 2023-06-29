//go:build integration

package migrationfixtures

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{Migration: &migrate.Migration{
		Id: "20200319130108_create_blobs_test_table",
		Up: []string{
			`CREATE TABLE IF NOT EXISTS blobs_test (
                id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                size bigint NOT NULL,
                created_at timestamp WITH time zone NOT NULL DEFAULT now(),
                marked_at timestamp WITH time zone,
                digest bytea NOT NULL,
                media_type text NOT NULL,
                CONSTRAINT pk_blobs_test PRIMARY KEY (id),
                CONSTRAINT unique_blobs_test_digest UNIQUE (digest),
                CONSTRAINT check_blobs_test_media_type_length CHECK ((char_length(media_type) <= 255))
            )`,
		},
		Down: []string{
			"DROP TABLE IF EXISTS blobs_test CASCADE",
		},
	}}

	allMigrations = append(allMigrations, m)
}

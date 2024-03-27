package migrations

import migrate "github.com/rubenv/sql-migrate"

// These media types are described in this document:
//
//	https://github.com/cue-lang/proposal/blob/3f04ccca5a27338829baadad415be5a6f55e614b/designs/modules/2449-modules-storage-model.md#detailed-design
func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240221102250_add_cue_module_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.cue.module.v1+json'), ('application/vnd.cue.modulefile.v1')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cue.module.v1+json',
						'application/vnd.cue.modulefile.v1'
					)`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}

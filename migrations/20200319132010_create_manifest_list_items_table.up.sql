CREATE TABLE IF NOT EXISTS manifest_list_items
(
    id               bigint                   NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    manifest_list_id bigint                   NOT NULL,
    manifest_id      bigint                   NOT NULL,
    created_at       timestamp with time zone NOT NULL DEFAULT now(),
    deleted_at       timestamp with time zone,
    CONSTRAINT pk_manifest_list_items PRIMARY KEY (id),
    CONSTRAINT fk_manifest_list_items_manifest_list_id FOREIGN KEY (manifest_list_id)
        REFERENCES manifest_lists (id) ON DELETE CASCADE,
    CONSTRAINT fk_manifest_list_items_manifest_id FOREIGN KEY (manifest_id)
        REFERENCES manifests (id) ON DELETE CASCADE,
    CONSTRAINT uq_manifest_list_items_manifest_list_id_manifest_id UNIQUE (manifest_list_id, manifest_id)
);
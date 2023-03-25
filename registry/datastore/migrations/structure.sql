SELECT
    pg_catalog.set_config('search_path', '', FALSE);

CREATE SCHEMA partitions;

CREATE SCHEMA public;

COMMENT ON SCHEMA public IS 'standard public schema';

CREATE FUNCTION public.gc_review_after (e text)
    RETURNS timestamp with time zone
    LANGUAGE plpgsql
    AS $$
DECLARE
    result timestamp with time zone;
    jitter_s interval;
BEGIN
    SELECT
        (random() * (60 - 5 + 1) + 5) * INTERVAL '1 second' INTO jitter_s;
    SELECT
        (now() + value) INTO result
    FROM
        gc_review_after_defaults
    WHERE
        event = e;
    IF result IS NULL THEN
        RETURN now() + interval '1 day' + jitter_s;
    ELSE
        RETURN result + jitter_s;
    END IF;
END;
$$;

CREATE FUNCTION public.gc_track_blob_uploads ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_blob_review_queue (digest, review_after, event)
        VALUES (NEW.digest, gc_review_after ('blob_upload'), 'blob_upload')
    ON CONFLICT (digest)
        DO UPDATE SET
            review_after = gc_review_after ('blob_upload'), event = 'blob_upload';
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_configuration_blobs ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.configuration_blob_digest IS NOT NULL THEN
        INSERT INTO gc_blobs_configurations (top_level_namespace_id, repository_id, manifest_id, digest)
            VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, NEW.configuration_blob_digest)
        ON CONFLICT (digest, manifest_id)
            DO NOTHING;
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_deleted_layers ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF (TG_LEVEL = 'STATEMENT') THEN
        INSERT INTO gc_blob_review_queue (digest, review_after, event)
        SELECT
            deleted_rows.digest,
            gc_review_after ('layer_delete'),
            'layer_delete'
        FROM
            old_table deleted_rows
        ORDER BY
            deleted_rows.digest ASC
        ON CONFLICT (digest)
            DO UPDATE SET
                review_after = gc_review_after ('layer_delete'),
                event = 'layer_delete';
    ELSIF (TG_LEVEL = 'ROW') THEN
        INSERT INTO gc_blob_review_queue (digest, review_after, event)
            VALUES (OLD.digest, gc_review_after ('layer_delete'), 'layer_delete')
        ON CONFLICT (digest)
            DO UPDATE SET
                review_after = gc_review_after ('layer_delete'), event = 'layer_delete';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_deleted_manifest_lists ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after, event)
        VALUES (OLD.top_level_namespace_id, OLD.repository_id, OLD.child_id, gc_review_after ('manifest_list_delete'), 'manifest_list_delete')
    ON CONFLICT (top_level_namespace_id, repository_id, manifest_id)
        DO UPDATE SET
            review_after = gc_review_after ('manifest_list_delete'), event = 'manifest_list_delete';
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_deleted_manifests ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.configuration_blob_digest IS NOT NULL THEN
        INSERT INTO gc_blob_review_queue (digest, review_after, event)
            VALUES (OLD.configuration_blob_digest, gc_review_after ('manifest_delete'), 'manifest_delete')
        ON CONFLICT (digest)
            DO UPDATE SET
                review_after = gc_review_after ('manifest_delete'), event = 'manifest_delete';
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_deleted_tags ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF EXISTS (
        SELECT
            1
        FROM
            manifests
        WHERE
            top_level_namespace_id = OLD.top_level_namespace_id
            AND repository_id = OLD.repository_id
            AND id = OLD.manifest_id) THEN
    INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after, event)
        VALUES (OLD.top_level_namespace_id, OLD.repository_id, OLD.manifest_id, gc_review_after ('tag_delete'), 'tag_delete')
    ON CONFLICT (top_level_namespace_id, repository_id, manifest_id)
        DO UPDATE SET
            review_after = gc_review_after ('tag_delete'), event = 'tag_delete';
END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_layer_blobs ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_blobs_layers (top_level_namespace_id, repository_id, layer_id, digest)
        VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, NEW.digest)
    ON CONFLICT (digest, layer_id)
        DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_manifest_uploads ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after, event)
        VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, gc_review_after ('manifest_upload'), 'manifest_upload');
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_switched_tags ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after, event)
        VALUES (OLD.top_level_namespace_id, OLD.repository_id, OLD.manifest_id, gc_review_after ('tag_switch'), 'tag_switch')
    ON CONFLICT (top_level_namespace_id, repository_id, manifest_id)
        DO UPDATE SET
            review_after = gc_review_after ('tag_switch'), event = 'tag_switch';
    RETURN NULL;
END;
$$;

CREATE FUNCTION public.gc_track_tmp_blobs_manifests ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO gc_tmp_blobs_manifests (digest)
        VALUES (NEW.digest)
    ON CONFLICT (digest)
        DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE TABLE public.blobs (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
)
PARTITION BY HASH (digest);

CREATE TABLE partitions.blobs_p_0 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.blobs_p_1 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.blobs_p_10 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.blobs_p_11 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.blobs_p_12 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.blobs_p_13 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.blobs_p_14 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.blobs_p_15 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.blobs_p_16 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.blobs_p_17 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.blobs_p_18 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.blobs_p_19 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.blobs_p_2 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.blobs_p_20 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.blobs_p_21 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.blobs_p_22 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.blobs_p_23 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.blobs_p_24 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.blobs_p_25 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.blobs_p_26 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.blobs_p_27 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.blobs_p_28 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.blobs_p_29 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.blobs_p_3 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.blobs_p_30 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.blobs_p_31 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.blobs_p_32 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.blobs_p_33 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.blobs_p_34 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.blobs_p_35 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.blobs_p_36 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.blobs_p_37 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.blobs_p_38 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.blobs_p_39 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.blobs_p_4 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.blobs_p_40 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.blobs_p_41 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.blobs_p_42 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.blobs_p_43 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.blobs_p_44 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.blobs_p_45 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.blobs_p_46 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.blobs_p_47 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.blobs_p_48 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.blobs_p_49 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.blobs_p_5 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.blobs_p_50 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.blobs_p_51 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.blobs_p_52 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.blobs_p_53 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.blobs_p_54 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.blobs_p_55 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.blobs_p_56 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.blobs_p_57 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.blobs_p_58 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.blobs_p_59 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.blobs_p_6 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.blobs_p_60 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.blobs_p_61 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.blobs_p_62 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.blobs_p_63 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.blobs_p_7 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.blobs_p_8 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.blobs_p_9 (
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.blobs ATTACH PARTITION partitions.blobs_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.gc_blobs_configurations (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
)
PARTITION BY HASH (digest);

CREATE TABLE partitions.gc_blobs_configurations_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.gc_blobs_configurations_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.gc_blobs_configurations_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.gc_blobs_configurations_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.gc_blobs_configurations_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.gc_blobs_configurations_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.gc_blobs_configurations_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.gc_blobs_configurations_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.gc_blobs_configurations_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.gc_blobs_configurations_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.gc_blobs_configurations_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.gc_blobs_configurations_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.gc_blobs_configurations_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.gc_blobs_configurations_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.gc_blobs_configurations_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.gc_blobs_configurations_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.gc_blobs_configurations_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.gc_blobs_configurations_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.gc_blobs_configurations_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.gc_blobs_configurations_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.gc_blobs_configurations_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.gc_blobs_configurations_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.gc_blobs_configurations_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.gc_blobs_configurations_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.gc_blobs_configurations_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.gc_blobs_configurations_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.gc_blobs_configurations_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.gc_blobs_configurations_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.gc_blobs_configurations_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.gc_blobs_configurations_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.gc_blobs_configurations_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.gc_blobs_configurations_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.gc_blobs_configurations_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.gc_blobs_configurations_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.gc_blobs_configurations_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.gc_blobs_configurations_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.gc_blobs_configurations_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.gc_blobs_configurations_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.gc_blobs_configurations_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.gc_blobs_configurations_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.gc_blobs_configurations_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.gc_blobs_configurations_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.gc_blobs_configurations_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.gc_blobs_configurations_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.gc_blobs_configurations_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.gc_blobs_configurations_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.gc_blobs_configurations_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.gc_blobs_configurations_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.gc_blobs_configurations_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.gc_blobs_configurations_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.gc_blobs_configurations_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.gc_blobs_configurations_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.gc_blobs_configurations_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.gc_blobs_configurations_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.gc_blobs_configurations_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.gc_blobs_configurations_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.gc_blobs_configurations_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.gc_blobs_configurations_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.gc_blobs_configurations_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.gc_blobs_configurations_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.gc_blobs_configurations_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.gc_blobs_configurations_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.gc_blobs_configurations_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.gc_blobs_configurations_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.gc_blobs_layers (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
)
PARTITION BY HASH (digest);

CREATE TABLE partitions.gc_blobs_layers_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.gc_blobs_layers_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.gc_blobs_layers_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.gc_blobs_layers_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.gc_blobs_layers_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.gc_blobs_layers_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.gc_blobs_layers_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.gc_blobs_layers_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.gc_blobs_layers_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.gc_blobs_layers_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.gc_blobs_layers_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.gc_blobs_layers_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.gc_blobs_layers_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.gc_blobs_layers_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.gc_blobs_layers_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.gc_blobs_layers_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.gc_blobs_layers_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.gc_blobs_layers_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.gc_blobs_layers_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.gc_blobs_layers_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.gc_blobs_layers_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.gc_blobs_layers_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.gc_blobs_layers_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.gc_blobs_layers_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.gc_blobs_layers_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.gc_blobs_layers_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.gc_blobs_layers_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.gc_blobs_layers_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.gc_blobs_layers_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.gc_blobs_layers_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.gc_blobs_layers_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.gc_blobs_layers_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.gc_blobs_layers_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.gc_blobs_layers_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.gc_blobs_layers_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.gc_blobs_layers_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.gc_blobs_layers_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.gc_blobs_layers_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.gc_blobs_layers_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.gc_blobs_layers_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.gc_blobs_layers_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.gc_blobs_layers_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.gc_blobs_layers_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.gc_blobs_layers_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.gc_blobs_layers_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.gc_blobs_layers_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.gc_blobs_layers_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.gc_blobs_layers_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.gc_blobs_layers_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.gc_blobs_layers_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.gc_blobs_layers_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.gc_blobs_layers_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.gc_blobs_layers_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.gc_blobs_layers_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.gc_blobs_layers_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.gc_blobs_layers_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.gc_blobs_layers_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.gc_blobs_layers_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.gc_blobs_layers_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.gc_blobs_layers_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.gc_blobs_layers_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.gc_blobs_layers_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.gc_blobs_layers_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.gc_blobs_layers_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    layer_id bigint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.layers (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
)
PARTITION BY HASH (top_level_namespace_id);

CREATE TABLE partitions.layers_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.layers_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.layers_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.layers_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.layers_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.layers_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.layers_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.layers_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.layers_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.layers_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.layers_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.layers_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.layers_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.layers_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.layers_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.layers_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.layers_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.layers_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.layers_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.layers_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.layers_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.layers_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.layers_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.layers_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.layers_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.layers_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.layers_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.layers_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.layers_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.layers_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.layers_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.layers_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.layers_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.layers_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.layers_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.layers_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.layers_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.layers_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.layers_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.layers_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.layers_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.layers_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.layers_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.layers_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.layers_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.layers_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.layers_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.layers_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.layers_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.layers_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.layers_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.layers_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.layers_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.layers_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.layers_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.layers_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.layers_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.layers_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.layers_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.layers_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.layers_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.layers_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.layers_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.layers_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    media_type_id smallint NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE ONLY public.layers ATTACH PARTITION partitions.layers_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.manifest_references (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
)
PARTITION BY HASH (top_level_namespace_id);

CREATE TABLE partitions.manifest_references_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.manifest_references_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.manifest_references_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.manifest_references_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.manifest_references_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.manifest_references_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.manifest_references_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.manifest_references_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.manifest_references_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.manifest_references_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.manifest_references_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.manifest_references_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.manifest_references_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.manifest_references_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.manifest_references_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.manifest_references_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.manifest_references_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.manifest_references_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.manifest_references_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.manifest_references_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.manifest_references_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.manifest_references_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.manifest_references_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.manifest_references_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.manifest_references_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.manifest_references_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.manifest_references_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.manifest_references_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.manifest_references_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.manifest_references_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.manifest_references_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.manifest_references_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.manifest_references_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.manifest_references_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.manifest_references_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.manifest_references_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.manifest_references_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.manifest_references_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.manifest_references_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.manifest_references_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.manifest_references_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.manifest_references_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.manifest_references_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.manifest_references_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.manifest_references_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.manifest_references_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.manifest_references_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.manifest_references_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.manifest_references_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.manifest_references_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.manifest_references_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.manifest_references_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.manifest_references_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.manifest_references_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.manifest_references_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.manifest_references_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.manifest_references_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.manifest_references_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.manifest_references_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.manifest_references_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.manifest_references_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.manifest_references_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.manifest_references_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.manifest_references_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    child_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_manifest_references_parent_id_and_child_id_differ CHECK ((parent_id <> child_id))
);

ALTER TABLE ONLY public.manifest_references ATTACH PARTITION partitions.manifest_references_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.manifests (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
)
PARTITION BY HASH (top_level_namespace_id);

CREATE TABLE partitions.manifests_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.manifests_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.manifests_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.manifests_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.manifests_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.manifests_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.manifests_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.manifests_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.manifests_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.manifests_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.manifests_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.manifests_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.manifests_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.manifests_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.manifests_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.manifests_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.manifests_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.manifests_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.manifests_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.manifests_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.manifests_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.manifests_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.manifests_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.manifests_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.manifests_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.manifests_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.manifests_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.manifests_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.manifests_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.manifests_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.manifests_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.manifests_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.manifests_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.manifests_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.manifests_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.manifests_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.manifests_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.manifests_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.manifests_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.manifests_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.manifests_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.manifests_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.manifests_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.manifests_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.manifests_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.manifests_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.manifests_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.manifests_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.manifests_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.manifests_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.manifests_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.manifests_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.manifests_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.manifests_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.manifests_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.manifests_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.manifests_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.manifests_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.manifests_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.manifests_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.manifests_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.manifests_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.manifests_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.manifests_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version smallint NOT NULL,
    media_type_id smallint NOT NULL,
    configuration_media_type_id smallint,
    configuration_payload bytea,
    configuration_blob_digest bytea,
    digest bytea NOT NULL,
    payload bytea NOT NULL,
    non_conformant boolean DEFAULT FALSE,
    total_size bigint NOT NULL,
    non_distributable_layers boolean DEFAULT FALSE
);

ALTER TABLE ONLY public.manifests ATTACH PARTITION partitions.manifests_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.repository_blobs (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
)
PARTITION BY HASH (top_level_namespace_id);

CREATE TABLE partitions.repository_blobs_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.repository_blobs_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.repository_blobs_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.repository_blobs_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.repository_blobs_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.repository_blobs_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.repository_blobs_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.repository_blobs_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.repository_blobs_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.repository_blobs_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.repository_blobs_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.repository_blobs_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.repository_blobs_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.repository_blobs_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.repository_blobs_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.repository_blobs_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.repository_blobs_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.repository_blobs_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.repository_blobs_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.repository_blobs_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.repository_blobs_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.repository_blobs_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.repository_blobs_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.repository_blobs_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.repository_blobs_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.repository_blobs_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.repository_blobs_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.repository_blobs_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.repository_blobs_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.repository_blobs_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.repository_blobs_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.repository_blobs_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.repository_blobs_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.repository_blobs_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.repository_blobs_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.repository_blobs_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.repository_blobs_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.repository_blobs_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.repository_blobs_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.repository_blobs_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.repository_blobs_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.repository_blobs_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.repository_blobs_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.repository_blobs_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.repository_blobs_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.repository_blobs_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.repository_blobs_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.repository_blobs_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.repository_blobs_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.repository_blobs_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.repository_blobs_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.repository_blobs_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.repository_blobs_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.repository_blobs_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.repository_blobs_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.repository_blobs_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.repository_blobs_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.repository_blobs_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.repository_blobs_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.repository_blobs_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.repository_blobs_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.repository_blobs_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.repository_blobs_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.repository_blobs_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    blob_digest bytea NOT NULL
);

ALTER TABLE ONLY public.repository_blobs ATTACH PARTITION partitions.repository_blobs_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.tags (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
)
PARTITION BY HASH (top_level_namespace_id);

CREATE TABLE partitions.tags_p_0 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_0
FOR VALUES WITH (MODULUS 64, REMAINDER 0);

CREATE TABLE partitions.tags_p_1 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_1
FOR VALUES WITH (MODULUS 64, REMAINDER 1);

CREATE TABLE partitions.tags_p_10 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_10
FOR VALUES WITH (MODULUS 64, REMAINDER 10);

CREATE TABLE partitions.tags_p_11 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_11
FOR VALUES WITH (MODULUS 64, REMAINDER 11);

CREATE TABLE partitions.tags_p_12 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_12
FOR VALUES WITH (MODULUS 64, REMAINDER 12);

CREATE TABLE partitions.tags_p_13 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_13
FOR VALUES WITH (MODULUS 64, REMAINDER 13);

CREATE TABLE partitions.tags_p_14 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_14
FOR VALUES WITH (MODULUS 64, REMAINDER 14);

CREATE TABLE partitions.tags_p_15 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_15
FOR VALUES WITH (MODULUS 64, REMAINDER 15);

CREATE TABLE partitions.tags_p_16 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_16
FOR VALUES WITH (MODULUS 64, REMAINDER 16);

CREATE TABLE partitions.tags_p_17 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_17
FOR VALUES WITH (MODULUS 64, REMAINDER 17);

CREATE TABLE partitions.tags_p_18 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_18
FOR VALUES WITH (MODULUS 64, REMAINDER 18);

CREATE TABLE partitions.tags_p_19 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_19
FOR VALUES WITH (MODULUS 64, REMAINDER 19);

CREATE TABLE partitions.tags_p_2 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_2
FOR VALUES WITH (MODULUS 64, REMAINDER 2);

CREATE TABLE partitions.tags_p_20 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_20
FOR VALUES WITH (MODULUS 64, REMAINDER 20);

CREATE TABLE partitions.tags_p_21 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_21
FOR VALUES WITH (MODULUS 64, REMAINDER 21);

CREATE TABLE partitions.tags_p_22 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_22
FOR VALUES WITH (MODULUS 64, REMAINDER 22);

CREATE TABLE partitions.tags_p_23 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_23
FOR VALUES WITH (MODULUS 64, REMAINDER 23);

CREATE TABLE partitions.tags_p_24 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_24
FOR VALUES WITH (MODULUS 64, REMAINDER 24);

CREATE TABLE partitions.tags_p_25 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_25
FOR VALUES WITH (MODULUS 64, REMAINDER 25);

CREATE TABLE partitions.tags_p_26 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_26
FOR VALUES WITH (MODULUS 64, REMAINDER 26);

CREATE TABLE partitions.tags_p_27 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_27
FOR VALUES WITH (MODULUS 64, REMAINDER 27);

CREATE TABLE partitions.tags_p_28 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_28
FOR VALUES WITH (MODULUS 64, REMAINDER 28);

CREATE TABLE partitions.tags_p_29 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_29
FOR VALUES WITH (MODULUS 64, REMAINDER 29);

CREATE TABLE partitions.tags_p_3 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_3
FOR VALUES WITH (MODULUS 64, REMAINDER 3);

CREATE TABLE partitions.tags_p_30 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_30
FOR VALUES WITH (MODULUS 64, REMAINDER 30);

CREATE TABLE partitions.tags_p_31 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_31
FOR VALUES WITH (MODULUS 64, REMAINDER 31);

CREATE TABLE partitions.tags_p_32 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_32
FOR VALUES WITH (MODULUS 64, REMAINDER 32);

CREATE TABLE partitions.tags_p_33 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_33
FOR VALUES WITH (MODULUS 64, REMAINDER 33);

CREATE TABLE partitions.tags_p_34 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_34
FOR VALUES WITH (MODULUS 64, REMAINDER 34);

CREATE TABLE partitions.tags_p_35 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_35
FOR VALUES WITH (MODULUS 64, REMAINDER 35);

CREATE TABLE partitions.tags_p_36 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_36
FOR VALUES WITH (MODULUS 64, REMAINDER 36);

CREATE TABLE partitions.tags_p_37 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_37
FOR VALUES WITH (MODULUS 64, REMAINDER 37);

CREATE TABLE partitions.tags_p_38 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_38
FOR VALUES WITH (MODULUS 64, REMAINDER 38);

CREATE TABLE partitions.tags_p_39 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_39
FOR VALUES WITH (MODULUS 64, REMAINDER 39);

CREATE TABLE partitions.tags_p_4 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_4
FOR VALUES WITH (MODULUS 64, REMAINDER 4);

CREATE TABLE partitions.tags_p_40 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_40
FOR VALUES WITH (MODULUS 64, REMAINDER 40);

CREATE TABLE partitions.tags_p_41 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_41
FOR VALUES WITH (MODULUS 64, REMAINDER 41);

CREATE TABLE partitions.tags_p_42 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_42
FOR VALUES WITH (MODULUS 64, REMAINDER 42);

CREATE TABLE partitions.tags_p_43 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_43
FOR VALUES WITH (MODULUS 64, REMAINDER 43);

CREATE TABLE partitions.tags_p_44 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_44
FOR VALUES WITH (MODULUS 64, REMAINDER 44);

CREATE TABLE partitions.tags_p_45 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_45
FOR VALUES WITH (MODULUS 64, REMAINDER 45);

CREATE TABLE partitions.tags_p_46 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_46
FOR VALUES WITH (MODULUS 64, REMAINDER 46);

CREATE TABLE partitions.tags_p_47 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_47
FOR VALUES WITH (MODULUS 64, REMAINDER 47);

CREATE TABLE partitions.tags_p_48 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_48
FOR VALUES WITH (MODULUS 64, REMAINDER 48);

CREATE TABLE partitions.tags_p_49 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_49
FOR VALUES WITH (MODULUS 64, REMAINDER 49);

CREATE TABLE partitions.tags_p_5 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_5
FOR VALUES WITH (MODULUS 64, REMAINDER 5);

CREATE TABLE partitions.tags_p_50 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_50
FOR VALUES WITH (MODULUS 64, REMAINDER 50);

CREATE TABLE partitions.tags_p_51 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_51
FOR VALUES WITH (MODULUS 64, REMAINDER 51);

CREATE TABLE partitions.tags_p_52 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_52
FOR VALUES WITH (MODULUS 64, REMAINDER 52);

CREATE TABLE partitions.tags_p_53 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_53
FOR VALUES WITH (MODULUS 64, REMAINDER 53);

CREATE TABLE partitions.tags_p_54 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_54
FOR VALUES WITH (MODULUS 64, REMAINDER 54);

CREATE TABLE partitions.tags_p_55 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_55
FOR VALUES WITH (MODULUS 64, REMAINDER 55);

CREATE TABLE partitions.tags_p_56 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_56
FOR VALUES WITH (MODULUS 64, REMAINDER 56);

CREATE TABLE partitions.tags_p_57 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_57
FOR VALUES WITH (MODULUS 64, REMAINDER 57);

CREATE TABLE partitions.tags_p_58 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_58
FOR VALUES WITH (MODULUS 64, REMAINDER 58);

CREATE TABLE partitions.tags_p_59 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_59
FOR VALUES WITH (MODULUS 64, REMAINDER 59);

CREATE TABLE partitions.tags_p_6 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_6
FOR VALUES WITH (MODULUS 64, REMAINDER 6);

CREATE TABLE partitions.tags_p_60 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_60
FOR VALUES WITH (MODULUS 64, REMAINDER 60);

CREATE TABLE partitions.tags_p_61 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_61
FOR VALUES WITH (MODULUS 64, REMAINDER 61);

CREATE TABLE partitions.tags_p_62 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_62
FOR VALUES WITH (MODULUS 64, REMAINDER 62);

CREATE TABLE partitions.tags_p_63 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_63
FOR VALUES WITH (MODULUS 64, REMAINDER 63);

CREATE TABLE partitions.tags_p_7 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_7
FOR VALUES WITH (MODULUS 64, REMAINDER 7);

CREATE TABLE partitions.tags_p_8 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_8
FOR VALUES WITH (MODULUS 64, REMAINDER 8);

CREATE TABLE partitions.tags_p_9 (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_tags_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE ONLY public.tags ATTACH PARTITION partitions.tags_p_9
FOR VALUES WITH (MODULUS 64, REMAINDER 9);

CREATE TABLE public.gc_blob_review_queue (
    review_after timestamp with time zone DEFAULT (now() + '1 day'::interval) NOT NULL,
    review_count integer DEFAULT 0 NOT NULL,
    digest bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    event text,
    CONSTRAINT check_gc_blob_review_queue_event_length CHECK ((char_length(event) <= 255)),
    CONSTRAINT check_gc_blob_review_queue_event_not_null CHECK ((event IS NOT NULL))
);

ALTER TABLE public.gc_blobs_configurations
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.gc_blobs_configurations_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

ALTER TABLE public.gc_blobs_layers
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.gc_blobs_layers_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

CREATE TABLE public.gc_manifest_review_queue (
    top_level_namespace_id bigint NOT NULL,
    repository_id bigint NOT NULL,
    manifest_id bigint NOT NULL,
    review_after timestamp with time zone DEFAULT (now() + '1 day'::interval) NOT NULL,
    review_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    event text,
    CONSTRAINT check_gc_manifest_review_queue_event_length CHECK ((char_length(event) <= 255)),
    CONSTRAINT check_gc_manifest_review_queue_event_not_null CHECK ((event IS NOT NULL))
);

CREATE TABLE public.gc_review_after_defaults (
    event text NOT NULL,
    value interval NOT NULL,
    CONSTRAINT check_gc_review_after_defaults_event_length CHECK ((char_length(event) <= 255))
);

CREATE TABLE public.gc_tmp_blobs_manifests (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    digest bytea NOT NULL
);

ALTER TABLE public.layers
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.layers_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

ALTER TABLE public.manifest_references
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.manifest_references_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

ALTER TABLE public.manifests
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.manifests_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

CREATE TABLE public.media_types (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    id smallint NOT NULL,
    media_type text NOT NULL,
    CONSTRAINT check_media_types_type_length CHECK ((char_length(media_type) <= 255))
);

ALTER TABLE public.media_types
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.media_types_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

CREATE TABLE public.repositories (
    id bigint NOT NULL,
    top_level_namespace_id bigint NOT NULL,
    parent_id bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    path text NOT NULL,
    migration_status text DEFAULT 'native' ::text NOT NULL,
    deleted_at timestamp with time zone,
    migration_error text,
    CONSTRAINT check_repositories_migration_error_length CHECK ((char_length(migration_error) <= 255)),
    CONSTRAINT check_repositories_migration_status_length CHECK ((char_length(migration_status) <= 255)),
    CONSTRAINT check_repositories_name_length CHECK ((char_length(name) <= 255)),
    CONSTRAINT check_repositories_path_length CHECK ((char_length(path) <= 255))
);

ALTER TABLE public.repositories
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.repositories_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

ALTER TABLE public.repository_blobs
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.repository_blobs_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

CREATE TABLE public.schema_migrations (
    id text NOT NULL,
    applied_at timestamp with time zone
);

ALTER TABLE public.tags
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.tags_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

CREATE TABLE public.top_level_namespaces (
    id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone,
    name text NOT NULL,
    CONSTRAINT check_top_level_namespaces_name_length CHECK ((char_length(name) <= 255))
);

ALTER TABLE public.top_level_namespaces
    ALTER COLUMN id
    ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME
        public.top_level_namespaces_id_seq START WITH 1 INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1);

ALTER TABLE ONLY public.blobs
    ADD CONSTRAINT pk_blobs PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_0
    ADD CONSTRAINT blobs_p_0_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_10
    ADD CONSTRAINT blobs_p_10_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_11
    ADD CONSTRAINT blobs_p_11_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_12
    ADD CONSTRAINT blobs_p_12_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_13
    ADD CONSTRAINT blobs_p_13_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_14
    ADD CONSTRAINT blobs_p_14_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_15
    ADD CONSTRAINT blobs_p_15_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_16
    ADD CONSTRAINT blobs_p_16_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_17
    ADD CONSTRAINT blobs_p_17_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_18
    ADD CONSTRAINT blobs_p_18_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_19
    ADD CONSTRAINT blobs_p_19_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_1
    ADD CONSTRAINT blobs_p_1_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_20
    ADD CONSTRAINT blobs_p_20_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_21
    ADD CONSTRAINT blobs_p_21_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_22
    ADD CONSTRAINT blobs_p_22_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_23
    ADD CONSTRAINT blobs_p_23_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_24
    ADD CONSTRAINT blobs_p_24_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_25
    ADD CONSTRAINT blobs_p_25_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_26
    ADD CONSTRAINT blobs_p_26_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_27
    ADD CONSTRAINT blobs_p_27_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_28
    ADD CONSTRAINT blobs_p_28_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_29
    ADD CONSTRAINT blobs_p_29_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_2
    ADD CONSTRAINT blobs_p_2_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_30
    ADD CONSTRAINT blobs_p_30_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_31
    ADD CONSTRAINT blobs_p_31_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_32
    ADD CONSTRAINT blobs_p_32_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_33
    ADD CONSTRAINT blobs_p_33_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_34
    ADD CONSTRAINT blobs_p_34_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_35
    ADD CONSTRAINT blobs_p_35_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_36
    ADD CONSTRAINT blobs_p_36_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_37
    ADD CONSTRAINT blobs_p_37_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_38
    ADD CONSTRAINT blobs_p_38_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_39
    ADD CONSTRAINT blobs_p_39_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_3
    ADD CONSTRAINT blobs_p_3_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_40
    ADD CONSTRAINT blobs_p_40_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_41
    ADD CONSTRAINT blobs_p_41_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_42
    ADD CONSTRAINT blobs_p_42_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_43
    ADD CONSTRAINT blobs_p_43_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_44
    ADD CONSTRAINT blobs_p_44_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_45
    ADD CONSTRAINT blobs_p_45_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_46
    ADD CONSTRAINT blobs_p_46_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_47
    ADD CONSTRAINT blobs_p_47_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_48
    ADD CONSTRAINT blobs_p_48_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_49
    ADD CONSTRAINT blobs_p_49_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_4
    ADD CONSTRAINT blobs_p_4_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_50
    ADD CONSTRAINT blobs_p_50_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_51
    ADD CONSTRAINT blobs_p_51_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_52
    ADD CONSTRAINT blobs_p_52_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_53
    ADD CONSTRAINT blobs_p_53_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_54
    ADD CONSTRAINT blobs_p_54_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_55
    ADD CONSTRAINT blobs_p_55_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_56
    ADD CONSTRAINT blobs_p_56_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_57
    ADD CONSTRAINT blobs_p_57_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_58
    ADD CONSTRAINT blobs_p_58_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_59
    ADD CONSTRAINT blobs_p_59_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_5
    ADD CONSTRAINT blobs_p_5_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_60
    ADD CONSTRAINT blobs_p_60_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_61
    ADD CONSTRAINT blobs_p_61_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_62
    ADD CONSTRAINT blobs_p_62_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_63
    ADD CONSTRAINT blobs_p_63_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_6
    ADD CONSTRAINT blobs_p_6_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_7
    ADD CONSTRAINT blobs_p_7_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_8
    ADD CONSTRAINT blobs_p_8_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY partitions.blobs_p_9
    ADD CONSTRAINT blobs_p_9_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY public.gc_blobs_configurations
    ADD CONSTRAINT unique_gc_blobs_configurations_digest_and_manifest_id UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_0
    ADD CONSTRAINT gc_blobs_configurations_p_0_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY public.gc_blobs_configurations
    ADD CONSTRAINT pk_gc_blobs_configurations PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_0
    ADD CONSTRAINT gc_blobs_configurations_p_0_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_10
    ADD CONSTRAINT gc_blobs_configurations_p_10_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_10
    ADD CONSTRAINT gc_blobs_configurations_p_10_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_11
    ADD CONSTRAINT gc_blobs_configurations_p_11_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_11
    ADD CONSTRAINT gc_blobs_configurations_p_11_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_12
    ADD CONSTRAINT gc_blobs_configurations_p_12_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_12
    ADD CONSTRAINT gc_blobs_configurations_p_12_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_13
    ADD CONSTRAINT gc_blobs_configurations_p_13_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_13
    ADD CONSTRAINT gc_blobs_configurations_p_13_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_14
    ADD CONSTRAINT gc_blobs_configurations_p_14_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_14
    ADD CONSTRAINT gc_blobs_configurations_p_14_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_15
    ADD CONSTRAINT gc_blobs_configurations_p_15_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_15
    ADD CONSTRAINT gc_blobs_configurations_p_15_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_16
    ADD CONSTRAINT gc_blobs_configurations_p_16_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_16
    ADD CONSTRAINT gc_blobs_configurations_p_16_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_17
    ADD CONSTRAINT gc_blobs_configurations_p_17_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_17
    ADD CONSTRAINT gc_blobs_configurations_p_17_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_18
    ADD CONSTRAINT gc_blobs_configurations_p_18_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_18
    ADD CONSTRAINT gc_blobs_configurations_p_18_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_19
    ADD CONSTRAINT gc_blobs_configurations_p_19_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_19
    ADD CONSTRAINT gc_blobs_configurations_p_19_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_1
    ADD CONSTRAINT gc_blobs_configurations_p_1_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_1
    ADD CONSTRAINT gc_blobs_configurations_p_1_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_20
    ADD CONSTRAINT gc_blobs_configurations_p_20_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_20
    ADD CONSTRAINT gc_blobs_configurations_p_20_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_21
    ADD CONSTRAINT gc_blobs_configurations_p_21_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_21
    ADD CONSTRAINT gc_blobs_configurations_p_21_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_22
    ADD CONSTRAINT gc_blobs_configurations_p_22_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_22
    ADD CONSTRAINT gc_blobs_configurations_p_22_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_23
    ADD CONSTRAINT gc_blobs_configurations_p_23_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_23
    ADD CONSTRAINT gc_blobs_configurations_p_23_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_24
    ADD CONSTRAINT gc_blobs_configurations_p_24_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_24
    ADD CONSTRAINT gc_blobs_configurations_p_24_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_25
    ADD CONSTRAINT gc_blobs_configurations_p_25_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_25
    ADD CONSTRAINT gc_blobs_configurations_p_25_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_26
    ADD CONSTRAINT gc_blobs_configurations_p_26_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_26
    ADD CONSTRAINT gc_blobs_configurations_p_26_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_27
    ADD CONSTRAINT gc_blobs_configurations_p_27_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_27
    ADD CONSTRAINT gc_blobs_configurations_p_27_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_28
    ADD CONSTRAINT gc_blobs_configurations_p_28_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_28
    ADD CONSTRAINT gc_blobs_configurations_p_28_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_29
    ADD CONSTRAINT gc_blobs_configurations_p_29_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_29
    ADD CONSTRAINT gc_blobs_configurations_p_29_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_2
    ADD CONSTRAINT gc_blobs_configurations_p_2_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_2
    ADD CONSTRAINT gc_blobs_configurations_p_2_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_30
    ADD CONSTRAINT gc_blobs_configurations_p_30_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_30
    ADD CONSTRAINT gc_blobs_configurations_p_30_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_31
    ADD CONSTRAINT gc_blobs_configurations_p_31_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_31
    ADD CONSTRAINT gc_blobs_configurations_p_31_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_32
    ADD CONSTRAINT gc_blobs_configurations_p_32_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_32
    ADD CONSTRAINT gc_blobs_configurations_p_32_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_33
    ADD CONSTRAINT gc_blobs_configurations_p_33_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_33
    ADD CONSTRAINT gc_blobs_configurations_p_33_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_34
    ADD CONSTRAINT gc_blobs_configurations_p_34_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_34
    ADD CONSTRAINT gc_blobs_configurations_p_34_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_35
    ADD CONSTRAINT gc_blobs_configurations_p_35_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_35
    ADD CONSTRAINT gc_blobs_configurations_p_35_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_36
    ADD CONSTRAINT gc_blobs_configurations_p_36_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_36
    ADD CONSTRAINT gc_blobs_configurations_p_36_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_37
    ADD CONSTRAINT gc_blobs_configurations_p_37_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_37
    ADD CONSTRAINT gc_blobs_configurations_p_37_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_38
    ADD CONSTRAINT gc_blobs_configurations_p_38_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_38
    ADD CONSTRAINT gc_blobs_configurations_p_38_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_39
    ADD CONSTRAINT gc_blobs_configurations_p_39_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_39
    ADD CONSTRAINT gc_blobs_configurations_p_39_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_3
    ADD CONSTRAINT gc_blobs_configurations_p_3_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_3
    ADD CONSTRAINT gc_blobs_configurations_p_3_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_40
    ADD CONSTRAINT gc_blobs_configurations_p_40_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_40
    ADD CONSTRAINT gc_blobs_configurations_p_40_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_41
    ADD CONSTRAINT gc_blobs_configurations_p_41_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_41
    ADD CONSTRAINT gc_blobs_configurations_p_41_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_42
    ADD CONSTRAINT gc_blobs_configurations_p_42_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_42
    ADD CONSTRAINT gc_blobs_configurations_p_42_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_43
    ADD CONSTRAINT gc_blobs_configurations_p_43_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_43
    ADD CONSTRAINT gc_blobs_configurations_p_43_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_44
    ADD CONSTRAINT gc_blobs_configurations_p_44_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_44
    ADD CONSTRAINT gc_blobs_configurations_p_44_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_45
    ADD CONSTRAINT gc_blobs_configurations_p_45_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_45
    ADD CONSTRAINT gc_blobs_configurations_p_45_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_46
    ADD CONSTRAINT gc_blobs_configurations_p_46_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_46
    ADD CONSTRAINT gc_blobs_configurations_p_46_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_47
    ADD CONSTRAINT gc_blobs_configurations_p_47_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_47
    ADD CONSTRAINT gc_blobs_configurations_p_47_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_48
    ADD CONSTRAINT gc_blobs_configurations_p_48_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_48
    ADD CONSTRAINT gc_blobs_configurations_p_48_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_49
    ADD CONSTRAINT gc_blobs_configurations_p_49_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_49
    ADD CONSTRAINT gc_blobs_configurations_p_49_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_4
    ADD CONSTRAINT gc_blobs_configurations_p_4_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_4
    ADD CONSTRAINT gc_blobs_configurations_p_4_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_50
    ADD CONSTRAINT gc_blobs_configurations_p_50_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_50
    ADD CONSTRAINT gc_blobs_configurations_p_50_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_51
    ADD CONSTRAINT gc_blobs_configurations_p_51_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_51
    ADD CONSTRAINT gc_blobs_configurations_p_51_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_52
    ADD CONSTRAINT gc_blobs_configurations_p_52_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_52
    ADD CONSTRAINT gc_blobs_configurations_p_52_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_53
    ADD CONSTRAINT gc_blobs_configurations_p_53_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_53
    ADD CONSTRAINT gc_blobs_configurations_p_53_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_54
    ADD CONSTRAINT gc_blobs_configurations_p_54_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_54
    ADD CONSTRAINT gc_blobs_configurations_p_54_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_55
    ADD CONSTRAINT gc_blobs_configurations_p_55_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_55
    ADD CONSTRAINT gc_blobs_configurations_p_55_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_56
    ADD CONSTRAINT gc_blobs_configurations_p_56_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_56
    ADD CONSTRAINT gc_blobs_configurations_p_56_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_57
    ADD CONSTRAINT gc_blobs_configurations_p_57_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_57
    ADD CONSTRAINT gc_blobs_configurations_p_57_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_58
    ADD CONSTRAINT gc_blobs_configurations_p_58_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_58
    ADD CONSTRAINT gc_blobs_configurations_p_58_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_59
    ADD CONSTRAINT gc_blobs_configurations_p_59_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_59
    ADD CONSTRAINT gc_blobs_configurations_p_59_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_5
    ADD CONSTRAINT gc_blobs_configurations_p_5_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_5
    ADD CONSTRAINT gc_blobs_configurations_p_5_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_60
    ADD CONSTRAINT gc_blobs_configurations_p_60_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_60
    ADD CONSTRAINT gc_blobs_configurations_p_60_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_61
    ADD CONSTRAINT gc_blobs_configurations_p_61_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_61
    ADD CONSTRAINT gc_blobs_configurations_p_61_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_62
    ADD CONSTRAINT gc_blobs_configurations_p_62_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_62
    ADD CONSTRAINT gc_blobs_configurations_p_62_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_63
    ADD CONSTRAINT gc_blobs_configurations_p_63_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_63
    ADD CONSTRAINT gc_blobs_configurations_p_63_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_6
    ADD CONSTRAINT gc_blobs_configurations_p_6_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_6
    ADD CONSTRAINT gc_blobs_configurations_p_6_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_7
    ADD CONSTRAINT gc_blobs_configurations_p_7_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_7
    ADD CONSTRAINT gc_blobs_configurations_p_7_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_8
    ADD CONSTRAINT gc_blobs_configurations_p_8_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_8
    ADD CONSTRAINT gc_blobs_configurations_p_8_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_9
    ADD CONSTRAINT gc_blobs_configurations_p_9_digest_manifest_id_key UNIQUE (digest, manifest_id);

ALTER TABLE ONLY partitions.gc_blobs_configurations_p_9
    ADD CONSTRAINT gc_blobs_configurations_p_9_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY public.gc_blobs_layers
    ADD CONSTRAINT unique_gc_blobs_layers_digest_and_layer_id UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_0
    ADD CONSTRAINT gc_blobs_layers_p_0_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY public.gc_blobs_layers
    ADD CONSTRAINT pk_gc_blobs_layers PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_0
    ADD CONSTRAINT gc_blobs_layers_p_0_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_10
    ADD CONSTRAINT gc_blobs_layers_p_10_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_10
    ADD CONSTRAINT gc_blobs_layers_p_10_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_11
    ADD CONSTRAINT gc_blobs_layers_p_11_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_11
    ADD CONSTRAINT gc_blobs_layers_p_11_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_12
    ADD CONSTRAINT gc_blobs_layers_p_12_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_12
    ADD CONSTRAINT gc_blobs_layers_p_12_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_13
    ADD CONSTRAINT gc_blobs_layers_p_13_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_13
    ADD CONSTRAINT gc_blobs_layers_p_13_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_14
    ADD CONSTRAINT gc_blobs_layers_p_14_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_14
    ADD CONSTRAINT gc_blobs_layers_p_14_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_15
    ADD CONSTRAINT gc_blobs_layers_p_15_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_15
    ADD CONSTRAINT gc_blobs_layers_p_15_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_16
    ADD CONSTRAINT gc_blobs_layers_p_16_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_16
    ADD CONSTRAINT gc_blobs_layers_p_16_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_17
    ADD CONSTRAINT gc_blobs_layers_p_17_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_17
    ADD CONSTRAINT gc_blobs_layers_p_17_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_18
    ADD CONSTRAINT gc_blobs_layers_p_18_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_18
    ADD CONSTRAINT gc_blobs_layers_p_18_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_19
    ADD CONSTRAINT gc_blobs_layers_p_19_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_19
    ADD CONSTRAINT gc_blobs_layers_p_19_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_1
    ADD CONSTRAINT gc_blobs_layers_p_1_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_1
    ADD CONSTRAINT gc_blobs_layers_p_1_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_20
    ADD CONSTRAINT gc_blobs_layers_p_20_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_20
    ADD CONSTRAINT gc_blobs_layers_p_20_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_21
    ADD CONSTRAINT gc_blobs_layers_p_21_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_21
    ADD CONSTRAINT gc_blobs_layers_p_21_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_22
    ADD CONSTRAINT gc_blobs_layers_p_22_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_22
    ADD CONSTRAINT gc_blobs_layers_p_22_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_23
    ADD CONSTRAINT gc_blobs_layers_p_23_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_23
    ADD CONSTRAINT gc_blobs_layers_p_23_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_24
    ADD CONSTRAINT gc_blobs_layers_p_24_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_24
    ADD CONSTRAINT gc_blobs_layers_p_24_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_25
    ADD CONSTRAINT gc_blobs_layers_p_25_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_25
    ADD CONSTRAINT gc_blobs_layers_p_25_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_26
    ADD CONSTRAINT gc_blobs_layers_p_26_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_26
    ADD CONSTRAINT gc_blobs_layers_p_26_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_27
    ADD CONSTRAINT gc_blobs_layers_p_27_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_27
    ADD CONSTRAINT gc_blobs_layers_p_27_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_28
    ADD CONSTRAINT gc_blobs_layers_p_28_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_28
    ADD CONSTRAINT gc_blobs_layers_p_28_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_29
    ADD CONSTRAINT gc_blobs_layers_p_29_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_29
    ADD CONSTRAINT gc_blobs_layers_p_29_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_2
    ADD CONSTRAINT gc_blobs_layers_p_2_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_2
    ADD CONSTRAINT gc_blobs_layers_p_2_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_30
    ADD CONSTRAINT gc_blobs_layers_p_30_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_30
    ADD CONSTRAINT gc_blobs_layers_p_30_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_31
    ADD CONSTRAINT gc_blobs_layers_p_31_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_31
    ADD CONSTRAINT gc_blobs_layers_p_31_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_32
    ADD CONSTRAINT gc_blobs_layers_p_32_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_32
    ADD CONSTRAINT gc_blobs_layers_p_32_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_33
    ADD CONSTRAINT gc_blobs_layers_p_33_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_33
    ADD CONSTRAINT gc_blobs_layers_p_33_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_34
    ADD CONSTRAINT gc_blobs_layers_p_34_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_34
    ADD CONSTRAINT gc_blobs_layers_p_34_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_35
    ADD CONSTRAINT gc_blobs_layers_p_35_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_35
    ADD CONSTRAINT gc_blobs_layers_p_35_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_36
    ADD CONSTRAINT gc_blobs_layers_p_36_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_36
    ADD CONSTRAINT gc_blobs_layers_p_36_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_37
    ADD CONSTRAINT gc_blobs_layers_p_37_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_37
    ADD CONSTRAINT gc_blobs_layers_p_37_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_38
    ADD CONSTRAINT gc_blobs_layers_p_38_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_38
    ADD CONSTRAINT gc_blobs_layers_p_38_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_39
    ADD CONSTRAINT gc_blobs_layers_p_39_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_39
    ADD CONSTRAINT gc_blobs_layers_p_39_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_3
    ADD CONSTRAINT gc_blobs_layers_p_3_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_3
    ADD CONSTRAINT gc_blobs_layers_p_3_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_40
    ADD CONSTRAINT gc_blobs_layers_p_40_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_40
    ADD CONSTRAINT gc_blobs_layers_p_40_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_41
    ADD CONSTRAINT gc_blobs_layers_p_41_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_41
    ADD CONSTRAINT gc_blobs_layers_p_41_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_42
    ADD CONSTRAINT gc_blobs_layers_p_42_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_42
    ADD CONSTRAINT gc_blobs_layers_p_42_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_43
    ADD CONSTRAINT gc_blobs_layers_p_43_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_43
    ADD CONSTRAINT gc_blobs_layers_p_43_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_44
    ADD CONSTRAINT gc_blobs_layers_p_44_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_44
    ADD CONSTRAINT gc_blobs_layers_p_44_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_45
    ADD CONSTRAINT gc_blobs_layers_p_45_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_45
    ADD CONSTRAINT gc_blobs_layers_p_45_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_46
    ADD CONSTRAINT gc_blobs_layers_p_46_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_46
    ADD CONSTRAINT gc_blobs_layers_p_46_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_47
    ADD CONSTRAINT gc_blobs_layers_p_47_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_47
    ADD CONSTRAINT gc_blobs_layers_p_47_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_48
    ADD CONSTRAINT gc_blobs_layers_p_48_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_48
    ADD CONSTRAINT gc_blobs_layers_p_48_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_49
    ADD CONSTRAINT gc_blobs_layers_p_49_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_49
    ADD CONSTRAINT gc_blobs_layers_p_49_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_4
    ADD CONSTRAINT gc_blobs_layers_p_4_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_4
    ADD CONSTRAINT gc_blobs_layers_p_4_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_50
    ADD CONSTRAINT gc_blobs_layers_p_50_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_50
    ADD CONSTRAINT gc_blobs_layers_p_50_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_51
    ADD CONSTRAINT gc_blobs_layers_p_51_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_51
    ADD CONSTRAINT gc_blobs_layers_p_51_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_52
    ADD CONSTRAINT gc_blobs_layers_p_52_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_52
    ADD CONSTRAINT gc_blobs_layers_p_52_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_53
    ADD CONSTRAINT gc_blobs_layers_p_53_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_53
    ADD CONSTRAINT gc_blobs_layers_p_53_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_54
    ADD CONSTRAINT gc_blobs_layers_p_54_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_54
    ADD CONSTRAINT gc_blobs_layers_p_54_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_55
    ADD CONSTRAINT gc_blobs_layers_p_55_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_55
    ADD CONSTRAINT gc_blobs_layers_p_55_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_56
    ADD CONSTRAINT gc_blobs_layers_p_56_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_56
    ADD CONSTRAINT gc_blobs_layers_p_56_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_57
    ADD CONSTRAINT gc_blobs_layers_p_57_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_57
    ADD CONSTRAINT gc_blobs_layers_p_57_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_58
    ADD CONSTRAINT gc_blobs_layers_p_58_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_58
    ADD CONSTRAINT gc_blobs_layers_p_58_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_59
    ADD CONSTRAINT gc_blobs_layers_p_59_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_59
    ADD CONSTRAINT gc_blobs_layers_p_59_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_5
    ADD CONSTRAINT gc_blobs_layers_p_5_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_5
    ADD CONSTRAINT gc_blobs_layers_p_5_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_60
    ADD CONSTRAINT gc_blobs_layers_p_60_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_60
    ADD CONSTRAINT gc_blobs_layers_p_60_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_61
    ADD CONSTRAINT gc_blobs_layers_p_61_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_61
    ADD CONSTRAINT gc_blobs_layers_p_61_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_62
    ADD CONSTRAINT gc_blobs_layers_p_62_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_62
    ADD CONSTRAINT gc_blobs_layers_p_62_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_63
    ADD CONSTRAINT gc_blobs_layers_p_63_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_63
    ADD CONSTRAINT gc_blobs_layers_p_63_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_6
    ADD CONSTRAINT gc_blobs_layers_p_6_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_6
    ADD CONSTRAINT gc_blobs_layers_p_6_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_7
    ADD CONSTRAINT gc_blobs_layers_p_7_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_7
    ADD CONSTRAINT gc_blobs_layers_p_7_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_8
    ADD CONSTRAINT gc_blobs_layers_p_8_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_8
    ADD CONSTRAINT gc_blobs_layers_p_8_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_9
    ADD CONSTRAINT gc_blobs_layers_p_9_digest_layer_id_key UNIQUE (digest, layer_id);

ALTER TABLE ONLY partitions.gc_blobs_layers_p_9
    ADD CONSTRAINT gc_blobs_layers_p_9_pkey PRIMARY KEY (digest, id);

ALTER TABLE ONLY public.layers
    ADD CONSTRAINT pk_layers PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_0
    ADD CONSTRAINT layers_p_0_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY public.layers
    ADD CONSTRAINT unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest UNIQUE (top_level_namespace_id, repository_id, id, digest);

COMMENT ON CONSTRAINT unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ON public.layers IS 'Unique constraint required to optimize the cascade on delete from manifests to gc_blobs_layers, through layers';

ALTER TABLE ONLY partitions.layers_p_0
    ADD CONSTRAINT layers_p_0_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY public.layers
    ADD CONSTRAINT unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_0
    ADD CONSTRAINT layers_p_0_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_10
    ADD CONSTRAINT layers_p_10_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_10
    ADD CONSTRAINT layers_p_10_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_10
    ADD CONSTRAINT layers_p_10_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_11
    ADD CONSTRAINT layers_p_11_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_11
    ADD CONSTRAINT layers_p_11_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_11
    ADD CONSTRAINT layers_p_11_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_12
    ADD CONSTRAINT layers_p_12_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_12
    ADD CONSTRAINT layers_p_12_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_12
    ADD CONSTRAINT layers_p_12_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_13
    ADD CONSTRAINT layers_p_13_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_13
    ADD CONSTRAINT layers_p_13_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_13
    ADD CONSTRAINT layers_p_13_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_14
    ADD CONSTRAINT layers_p_14_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_14
    ADD CONSTRAINT layers_p_14_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_14
    ADD CONSTRAINT layers_p_14_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_15
    ADD CONSTRAINT layers_p_15_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_15
    ADD CONSTRAINT layers_p_15_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_15
    ADD CONSTRAINT layers_p_15_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_16
    ADD CONSTRAINT layers_p_16_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_16
    ADD CONSTRAINT layers_p_16_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_16
    ADD CONSTRAINT layers_p_16_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_17
    ADD CONSTRAINT layers_p_17_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_17
    ADD CONSTRAINT layers_p_17_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_17
    ADD CONSTRAINT layers_p_17_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_18
    ADD CONSTRAINT layers_p_18_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_18
    ADD CONSTRAINT layers_p_18_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_18
    ADD CONSTRAINT layers_p_18_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_19
    ADD CONSTRAINT layers_p_19_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_19
    ADD CONSTRAINT layers_p_19_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_19
    ADD CONSTRAINT layers_p_19_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_1
    ADD CONSTRAINT layers_p_1_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_1
    ADD CONSTRAINT layers_p_1_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_1
    ADD CONSTRAINT layers_p_1_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_20
    ADD CONSTRAINT layers_p_20_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_20
    ADD CONSTRAINT layers_p_20_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_20
    ADD CONSTRAINT layers_p_20_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_21
    ADD CONSTRAINT layers_p_21_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_21
    ADD CONSTRAINT layers_p_21_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_21
    ADD CONSTRAINT layers_p_21_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_22
    ADD CONSTRAINT layers_p_22_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_22
    ADD CONSTRAINT layers_p_22_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_22
    ADD CONSTRAINT layers_p_22_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_23
    ADD CONSTRAINT layers_p_23_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_23
    ADD CONSTRAINT layers_p_23_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_23
    ADD CONSTRAINT layers_p_23_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_24
    ADD CONSTRAINT layers_p_24_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_24
    ADD CONSTRAINT layers_p_24_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_24
    ADD CONSTRAINT layers_p_24_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_25
    ADD CONSTRAINT layers_p_25_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_25
    ADD CONSTRAINT layers_p_25_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_25
    ADD CONSTRAINT layers_p_25_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_26
    ADD CONSTRAINT layers_p_26_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_26
    ADD CONSTRAINT layers_p_26_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_26
    ADD CONSTRAINT layers_p_26_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_27
    ADD CONSTRAINT layers_p_27_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_27
    ADD CONSTRAINT layers_p_27_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_27
    ADD CONSTRAINT layers_p_27_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_28
    ADD CONSTRAINT layers_p_28_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_28
    ADD CONSTRAINT layers_p_28_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_28
    ADD CONSTRAINT layers_p_28_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_29
    ADD CONSTRAINT layers_p_29_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_29
    ADD CONSTRAINT layers_p_29_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_29
    ADD CONSTRAINT layers_p_29_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_2
    ADD CONSTRAINT layers_p_2_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_2
    ADD CONSTRAINT layers_p_2_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_2
    ADD CONSTRAINT layers_p_2_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_30
    ADD CONSTRAINT layers_p_30_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_30
    ADD CONSTRAINT layers_p_30_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_30
    ADD CONSTRAINT layers_p_30_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_31
    ADD CONSTRAINT layers_p_31_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_31
    ADD CONSTRAINT layers_p_31_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_31
    ADD CONSTRAINT layers_p_31_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_32
    ADD CONSTRAINT layers_p_32_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_32
    ADD CONSTRAINT layers_p_32_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_32
    ADD CONSTRAINT layers_p_32_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_33
    ADD CONSTRAINT layers_p_33_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_33
    ADD CONSTRAINT layers_p_33_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_33
    ADD CONSTRAINT layers_p_33_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_34
    ADD CONSTRAINT layers_p_34_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_34
    ADD CONSTRAINT layers_p_34_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_34
    ADD CONSTRAINT layers_p_34_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_35
    ADD CONSTRAINT layers_p_35_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_35
    ADD CONSTRAINT layers_p_35_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_35
    ADD CONSTRAINT layers_p_35_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_36
    ADD CONSTRAINT layers_p_36_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_36
    ADD CONSTRAINT layers_p_36_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_36
    ADD CONSTRAINT layers_p_36_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_37
    ADD CONSTRAINT layers_p_37_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_37
    ADD CONSTRAINT layers_p_37_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_37
    ADD CONSTRAINT layers_p_37_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_38
    ADD CONSTRAINT layers_p_38_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_38
    ADD CONSTRAINT layers_p_38_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_38
    ADD CONSTRAINT layers_p_38_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_39
    ADD CONSTRAINT layers_p_39_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_39
    ADD CONSTRAINT layers_p_39_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_39
    ADD CONSTRAINT layers_p_39_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_3
    ADD CONSTRAINT layers_p_3_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_3
    ADD CONSTRAINT layers_p_3_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_3
    ADD CONSTRAINT layers_p_3_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_40
    ADD CONSTRAINT layers_p_40_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_40
    ADD CONSTRAINT layers_p_40_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_40
    ADD CONSTRAINT layers_p_40_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_41
    ADD CONSTRAINT layers_p_41_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_41
    ADD CONSTRAINT layers_p_41_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_41
    ADD CONSTRAINT layers_p_41_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_42
    ADD CONSTRAINT layers_p_42_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_42
    ADD CONSTRAINT layers_p_42_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_42
    ADD CONSTRAINT layers_p_42_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_43
    ADD CONSTRAINT layers_p_43_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_43
    ADD CONSTRAINT layers_p_43_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_43
    ADD CONSTRAINT layers_p_43_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_44
    ADD CONSTRAINT layers_p_44_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_44
    ADD CONSTRAINT layers_p_44_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_44
    ADD CONSTRAINT layers_p_44_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_45
    ADD CONSTRAINT layers_p_45_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_45
    ADD CONSTRAINT layers_p_45_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_45
    ADD CONSTRAINT layers_p_45_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_46
    ADD CONSTRAINT layers_p_46_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_46
    ADD CONSTRAINT layers_p_46_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_46
    ADD CONSTRAINT layers_p_46_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_47
    ADD CONSTRAINT layers_p_47_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_47
    ADD CONSTRAINT layers_p_47_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_47
    ADD CONSTRAINT layers_p_47_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_48
    ADD CONSTRAINT layers_p_48_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_48
    ADD CONSTRAINT layers_p_48_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_48
    ADD CONSTRAINT layers_p_48_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_49
    ADD CONSTRAINT layers_p_49_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_49
    ADD CONSTRAINT layers_p_49_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_49
    ADD CONSTRAINT layers_p_49_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_4
    ADD CONSTRAINT layers_p_4_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_4
    ADD CONSTRAINT layers_p_4_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_4
    ADD CONSTRAINT layers_p_4_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_50
    ADD CONSTRAINT layers_p_50_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_50
    ADD CONSTRAINT layers_p_50_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_50
    ADD CONSTRAINT layers_p_50_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_51
    ADD CONSTRAINT layers_p_51_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_51
    ADD CONSTRAINT layers_p_51_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_51
    ADD CONSTRAINT layers_p_51_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_52
    ADD CONSTRAINT layers_p_52_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_52
    ADD CONSTRAINT layers_p_52_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_52
    ADD CONSTRAINT layers_p_52_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_53
    ADD CONSTRAINT layers_p_53_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_53
    ADD CONSTRAINT layers_p_53_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_53
    ADD CONSTRAINT layers_p_53_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_54
    ADD CONSTRAINT layers_p_54_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_54
    ADD CONSTRAINT layers_p_54_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_54
    ADD CONSTRAINT layers_p_54_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_55
    ADD CONSTRAINT layers_p_55_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_55
    ADD CONSTRAINT layers_p_55_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_55
    ADD CONSTRAINT layers_p_55_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_56
    ADD CONSTRAINT layers_p_56_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_56
    ADD CONSTRAINT layers_p_56_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_56
    ADD CONSTRAINT layers_p_56_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_57
    ADD CONSTRAINT layers_p_57_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_57
    ADD CONSTRAINT layers_p_57_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_57
    ADD CONSTRAINT layers_p_57_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_58
    ADD CONSTRAINT layers_p_58_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_58
    ADD CONSTRAINT layers_p_58_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_58
    ADD CONSTRAINT layers_p_58_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_59
    ADD CONSTRAINT layers_p_59_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_59
    ADD CONSTRAINT layers_p_59_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_59
    ADD CONSTRAINT layers_p_59_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_5
    ADD CONSTRAINT layers_p_5_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_5
    ADD CONSTRAINT layers_p_5_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_5
    ADD CONSTRAINT layers_p_5_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_60
    ADD CONSTRAINT layers_p_60_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_60
    ADD CONSTRAINT layers_p_60_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_60
    ADD CONSTRAINT layers_p_60_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_61
    ADD CONSTRAINT layers_p_61_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_61
    ADD CONSTRAINT layers_p_61_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_61
    ADD CONSTRAINT layers_p_61_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_62
    ADD CONSTRAINT layers_p_62_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_62
    ADD CONSTRAINT layers_p_62_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_62
    ADD CONSTRAINT layers_p_62_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_63
    ADD CONSTRAINT layers_p_63_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_63
    ADD CONSTRAINT layers_p_63_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_63
    ADD CONSTRAINT layers_p_63_top_level_namespace_id_repository_id_manifest_i_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_6
    ADD CONSTRAINT layers_p_6_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_6
    ADD CONSTRAINT layers_p_6_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_6
    ADD CONSTRAINT layers_p_6_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_7
    ADD CONSTRAINT layers_p_7_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_7
    ADD CONSTRAINT layers_p_7_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_7
    ADD CONSTRAINT layers_p_7_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_8
    ADD CONSTRAINT layers_p_8_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_8
    ADD CONSTRAINT layers_p_8_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_8
    ADD CONSTRAINT layers_p_8_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY partitions.layers_p_9
    ADD CONSTRAINT layers_p_9_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.layers_p_9
    ADD CONSTRAINT layers_p_9_top_level_namespace_id_repository_id_id_digest_key UNIQUE (top_level_namespace_id, repository_id, id, digest);

ALTER TABLE ONLY partitions.layers_p_9
    ADD CONSTRAINT layers_p_9_top_level_namespace_id_repository_id_manifest_id_key UNIQUE (top_level_namespace_id, repository_id, manifest_id, digest);

ALTER TABLE ONLY public.manifest_references
    ADD CONSTRAINT pk_manifest_references PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_0
    ADD CONSTRAINT manifest_references_p_0_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY public.manifest_references
    ADD CONSTRAINT unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_0
    ADD CONSTRAINT manifest_references_p_0_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_10
    ADD CONSTRAINT manifest_references_p_10_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_10
    ADD CONSTRAINT manifest_references_p_10_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_11
    ADD CONSTRAINT manifest_references_p_11_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_11
    ADD CONSTRAINT manifest_references_p_11_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_12
    ADD CONSTRAINT manifest_references_p_12_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_12
    ADD CONSTRAINT manifest_references_p_12_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_13
    ADD CONSTRAINT manifest_references_p_13_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_13
    ADD CONSTRAINT manifest_references_p_13_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_14
    ADD CONSTRAINT manifest_references_p_14_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_14
    ADD CONSTRAINT manifest_references_p_14_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_15
    ADD CONSTRAINT manifest_references_p_15_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_15
    ADD CONSTRAINT manifest_references_p_15_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_16
    ADD CONSTRAINT manifest_references_p_16_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_16
    ADD CONSTRAINT manifest_references_p_16_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_17
    ADD CONSTRAINT manifest_references_p_17_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_17
    ADD CONSTRAINT manifest_references_p_17_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_18
    ADD CONSTRAINT manifest_references_p_18_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_18
    ADD CONSTRAINT manifest_references_p_18_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_19
    ADD CONSTRAINT manifest_references_p_19_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_19
    ADD CONSTRAINT manifest_references_p_19_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_1
    ADD CONSTRAINT manifest_references_p_1_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_1
    ADD CONSTRAINT manifest_references_p_1_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_20
    ADD CONSTRAINT manifest_references_p_20_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_20
    ADD CONSTRAINT manifest_references_p_20_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_21
    ADD CONSTRAINT manifest_references_p_21_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_21
    ADD CONSTRAINT manifest_references_p_21_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_22
    ADD CONSTRAINT manifest_references_p_22_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_22
    ADD CONSTRAINT manifest_references_p_22_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_23
    ADD CONSTRAINT manifest_references_p_23_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_23
    ADD CONSTRAINT manifest_references_p_23_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_24
    ADD CONSTRAINT manifest_references_p_24_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_24
    ADD CONSTRAINT manifest_references_p_24_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_25
    ADD CONSTRAINT manifest_references_p_25_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_25
    ADD CONSTRAINT manifest_references_p_25_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_26
    ADD CONSTRAINT manifest_references_p_26_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_26
    ADD CONSTRAINT manifest_references_p_26_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_27
    ADD CONSTRAINT manifest_references_p_27_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_27
    ADD CONSTRAINT manifest_references_p_27_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_28
    ADD CONSTRAINT manifest_references_p_28_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_28
    ADD CONSTRAINT manifest_references_p_28_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_29
    ADD CONSTRAINT manifest_references_p_29_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_29
    ADD CONSTRAINT manifest_references_p_29_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_2
    ADD CONSTRAINT manifest_references_p_2_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_2
    ADD CONSTRAINT manifest_references_p_2_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_30
    ADD CONSTRAINT manifest_references_p_30_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_30
    ADD CONSTRAINT manifest_references_p_30_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_31
    ADD CONSTRAINT manifest_references_p_31_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_31
    ADD CONSTRAINT manifest_references_p_31_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_32
    ADD CONSTRAINT manifest_references_p_32_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_32
    ADD CONSTRAINT manifest_references_p_32_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_33
    ADD CONSTRAINT manifest_references_p_33_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_33
    ADD CONSTRAINT manifest_references_p_33_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_34
    ADD CONSTRAINT manifest_references_p_34_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_34
    ADD CONSTRAINT manifest_references_p_34_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_35
    ADD CONSTRAINT manifest_references_p_35_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_35
    ADD CONSTRAINT manifest_references_p_35_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_36
    ADD CONSTRAINT manifest_references_p_36_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_36
    ADD CONSTRAINT manifest_references_p_36_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_37
    ADD CONSTRAINT manifest_references_p_37_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_37
    ADD CONSTRAINT manifest_references_p_37_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_38
    ADD CONSTRAINT manifest_references_p_38_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_38
    ADD CONSTRAINT manifest_references_p_38_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_39
    ADD CONSTRAINT manifest_references_p_39_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_39
    ADD CONSTRAINT manifest_references_p_39_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_3
    ADD CONSTRAINT manifest_references_p_3_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_3
    ADD CONSTRAINT manifest_references_p_3_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_40
    ADD CONSTRAINT manifest_references_p_40_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_40
    ADD CONSTRAINT manifest_references_p_40_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_41
    ADD CONSTRAINT manifest_references_p_41_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_41
    ADD CONSTRAINT manifest_references_p_41_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_42
    ADD CONSTRAINT manifest_references_p_42_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_42
    ADD CONSTRAINT manifest_references_p_42_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_43
    ADD CONSTRAINT manifest_references_p_43_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_43
    ADD CONSTRAINT manifest_references_p_43_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_44
    ADD CONSTRAINT manifest_references_p_44_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_44
    ADD CONSTRAINT manifest_references_p_44_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_45
    ADD CONSTRAINT manifest_references_p_45_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_45
    ADD CONSTRAINT manifest_references_p_45_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_46
    ADD CONSTRAINT manifest_references_p_46_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_46
    ADD CONSTRAINT manifest_references_p_46_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_47
    ADD CONSTRAINT manifest_references_p_47_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_47
    ADD CONSTRAINT manifest_references_p_47_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_48
    ADD CONSTRAINT manifest_references_p_48_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_48
    ADD CONSTRAINT manifest_references_p_48_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_49
    ADD CONSTRAINT manifest_references_p_49_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_49
    ADD CONSTRAINT manifest_references_p_49_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_4
    ADD CONSTRAINT manifest_references_p_4_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_4
    ADD CONSTRAINT manifest_references_p_4_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_50
    ADD CONSTRAINT manifest_references_p_50_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_50
    ADD CONSTRAINT manifest_references_p_50_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_51
    ADD CONSTRAINT manifest_references_p_51_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_51
    ADD CONSTRAINT manifest_references_p_51_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_52
    ADD CONSTRAINT manifest_references_p_52_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_52
    ADD CONSTRAINT manifest_references_p_52_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_53
    ADD CONSTRAINT manifest_references_p_53_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_53
    ADD CONSTRAINT manifest_references_p_53_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_54
    ADD CONSTRAINT manifest_references_p_54_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_54
    ADD CONSTRAINT manifest_references_p_54_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_55
    ADD CONSTRAINT manifest_references_p_55_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_55
    ADD CONSTRAINT manifest_references_p_55_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_56
    ADD CONSTRAINT manifest_references_p_56_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_56
    ADD CONSTRAINT manifest_references_p_56_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_57
    ADD CONSTRAINT manifest_references_p_57_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_57
    ADD CONSTRAINT manifest_references_p_57_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_58
    ADD CONSTRAINT manifest_references_p_58_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_58
    ADD CONSTRAINT manifest_references_p_58_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_59
    ADD CONSTRAINT manifest_references_p_59_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_59
    ADD CONSTRAINT manifest_references_p_59_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_5
    ADD CONSTRAINT manifest_references_p_5_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_5
    ADD CONSTRAINT manifest_references_p_5_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_60
    ADD CONSTRAINT manifest_references_p_60_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_60
    ADD CONSTRAINT manifest_references_p_60_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_61
    ADD CONSTRAINT manifest_references_p_61_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_61
    ADD CONSTRAINT manifest_references_p_61_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_62
    ADD CONSTRAINT manifest_references_p_62_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_62
    ADD CONSTRAINT manifest_references_p_62_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_63
    ADD CONSTRAINT manifest_references_p_63_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_63
    ADD CONSTRAINT manifest_references_p_63_top_level_namespace_id_repository__key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_6
    ADD CONSTRAINT manifest_references_p_6_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_6
    ADD CONSTRAINT manifest_references_p_6_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_7
    ADD CONSTRAINT manifest_references_p_7_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_7
    ADD CONSTRAINT manifest_references_p_7_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_8
    ADD CONSTRAINT manifest_references_p_8_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_8
    ADD CONSTRAINT manifest_references_p_8_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY partitions.manifest_references_p_9
    ADD CONSTRAINT manifest_references_p_9_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifest_references_p_9
    ADD CONSTRAINT manifest_references_p_9_top_level_namespace_id_repository_i_key UNIQUE (top_level_namespace_id, repository_id, parent_id, child_id);

ALTER TABLE ONLY public.manifests
    ADD CONSTRAINT pk_manifests PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_0
    ADD CONSTRAINT manifests_p_0_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY public.manifests
    ADD CONSTRAINT unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_0
    ADD CONSTRAINT manifests_p_0_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY public.manifests
    ADD CONSTRAINT unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

COMMENT ON CONSTRAINT unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ON public.manifests IS 'Unique constraint required to optimize the cascade on delete from manifests to gc_blobs_configurations';

ALTER TABLE ONLY partitions.manifests_p_0
    ADD CONSTRAINT manifests_p_0_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_10
    ADD CONSTRAINT manifests_p_10_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_10
    ADD CONSTRAINT manifests_p_10_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_10
    ADD CONSTRAINT manifests_p_10_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_11
    ADD CONSTRAINT manifests_p_11_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_11
    ADD CONSTRAINT manifests_p_11_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_11
    ADD CONSTRAINT manifests_p_11_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_12
    ADD CONSTRAINT manifests_p_12_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_12
    ADD CONSTRAINT manifests_p_12_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_12
    ADD CONSTRAINT manifests_p_12_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_13
    ADD CONSTRAINT manifests_p_13_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_13
    ADD CONSTRAINT manifests_p_13_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_13
    ADD CONSTRAINT manifests_p_13_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_14
    ADD CONSTRAINT manifests_p_14_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_14
    ADD CONSTRAINT manifests_p_14_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_14
    ADD CONSTRAINT manifests_p_14_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_15
    ADD CONSTRAINT manifests_p_15_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_15
    ADD CONSTRAINT manifests_p_15_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_15
    ADD CONSTRAINT manifests_p_15_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_16
    ADD CONSTRAINT manifests_p_16_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_16
    ADD CONSTRAINT manifests_p_16_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_16
    ADD CONSTRAINT manifests_p_16_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_17
    ADD CONSTRAINT manifests_p_17_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_17
    ADD CONSTRAINT manifests_p_17_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_17
    ADD CONSTRAINT manifests_p_17_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_18
    ADD CONSTRAINT manifests_p_18_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_18
    ADD CONSTRAINT manifests_p_18_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_18
    ADD CONSTRAINT manifests_p_18_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_19
    ADD CONSTRAINT manifests_p_19_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_19
    ADD CONSTRAINT manifests_p_19_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_19
    ADD CONSTRAINT manifests_p_19_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_1
    ADD CONSTRAINT manifests_p_1_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_1
    ADD CONSTRAINT manifests_p_1_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_1
    ADD CONSTRAINT manifests_p_1_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_20
    ADD CONSTRAINT manifests_p_20_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_20
    ADD CONSTRAINT manifests_p_20_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_20
    ADD CONSTRAINT manifests_p_20_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_21
    ADD CONSTRAINT manifests_p_21_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_21
    ADD CONSTRAINT manifests_p_21_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_21
    ADD CONSTRAINT manifests_p_21_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_22
    ADD CONSTRAINT manifests_p_22_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_22
    ADD CONSTRAINT manifests_p_22_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_22
    ADD CONSTRAINT manifests_p_22_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_23
    ADD CONSTRAINT manifests_p_23_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_23
    ADD CONSTRAINT manifests_p_23_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_23
    ADD CONSTRAINT manifests_p_23_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_24
    ADD CONSTRAINT manifests_p_24_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_24
    ADD CONSTRAINT manifests_p_24_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_24
    ADD CONSTRAINT manifests_p_24_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_25
    ADD CONSTRAINT manifests_p_25_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_25
    ADD CONSTRAINT manifests_p_25_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_25
    ADD CONSTRAINT manifests_p_25_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_26
    ADD CONSTRAINT manifests_p_26_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_26
    ADD CONSTRAINT manifests_p_26_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_26
    ADD CONSTRAINT manifests_p_26_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_27
    ADD CONSTRAINT manifests_p_27_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_27
    ADD CONSTRAINT manifests_p_27_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_27
    ADD CONSTRAINT manifests_p_27_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_28
    ADD CONSTRAINT manifests_p_28_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_28
    ADD CONSTRAINT manifests_p_28_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_28
    ADD CONSTRAINT manifests_p_28_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_29
    ADD CONSTRAINT manifests_p_29_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_29
    ADD CONSTRAINT manifests_p_29_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_29
    ADD CONSTRAINT manifests_p_29_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_2
    ADD CONSTRAINT manifests_p_2_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_2
    ADD CONSTRAINT manifests_p_2_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_2
    ADD CONSTRAINT manifests_p_2_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_30
    ADD CONSTRAINT manifests_p_30_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_30
    ADD CONSTRAINT manifests_p_30_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_30
    ADD CONSTRAINT manifests_p_30_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_31
    ADD CONSTRAINT manifests_p_31_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_31
    ADD CONSTRAINT manifests_p_31_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_31
    ADD CONSTRAINT manifests_p_31_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_32
    ADD CONSTRAINT manifests_p_32_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_32
    ADD CONSTRAINT manifests_p_32_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_32
    ADD CONSTRAINT manifests_p_32_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_33
    ADD CONSTRAINT manifests_p_33_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_33
    ADD CONSTRAINT manifests_p_33_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_33
    ADD CONSTRAINT manifests_p_33_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_34
    ADD CONSTRAINT manifests_p_34_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_34
    ADD CONSTRAINT manifests_p_34_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_34
    ADD CONSTRAINT manifests_p_34_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_35
    ADD CONSTRAINT manifests_p_35_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_35
    ADD CONSTRAINT manifests_p_35_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_35
    ADD CONSTRAINT manifests_p_35_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_36
    ADD CONSTRAINT manifests_p_36_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_36
    ADD CONSTRAINT manifests_p_36_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_36
    ADD CONSTRAINT manifests_p_36_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_37
    ADD CONSTRAINT manifests_p_37_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_37
    ADD CONSTRAINT manifests_p_37_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_37
    ADD CONSTRAINT manifests_p_37_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_38
    ADD CONSTRAINT manifests_p_38_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_38
    ADD CONSTRAINT manifests_p_38_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_38
    ADD CONSTRAINT manifests_p_38_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_39
    ADD CONSTRAINT manifests_p_39_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_39
    ADD CONSTRAINT manifests_p_39_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_39
    ADD CONSTRAINT manifests_p_39_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_3
    ADD CONSTRAINT manifests_p_3_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_3
    ADD CONSTRAINT manifests_p_3_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_3
    ADD CONSTRAINT manifests_p_3_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_40
    ADD CONSTRAINT manifests_p_40_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_40
    ADD CONSTRAINT manifests_p_40_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_40
    ADD CONSTRAINT manifests_p_40_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_41
    ADD CONSTRAINT manifests_p_41_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_41
    ADD CONSTRAINT manifests_p_41_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_41
    ADD CONSTRAINT manifests_p_41_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_42
    ADD CONSTRAINT manifests_p_42_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_42
    ADD CONSTRAINT manifests_p_42_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_42
    ADD CONSTRAINT manifests_p_42_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_43
    ADD CONSTRAINT manifests_p_43_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_43
    ADD CONSTRAINT manifests_p_43_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_43
    ADD CONSTRAINT manifests_p_43_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_44
    ADD CONSTRAINT manifests_p_44_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_44
    ADD CONSTRAINT manifests_p_44_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_44
    ADD CONSTRAINT manifests_p_44_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_45
    ADD CONSTRAINT manifests_p_45_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_45
    ADD CONSTRAINT manifests_p_45_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_45
    ADD CONSTRAINT manifests_p_45_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_46
    ADD CONSTRAINT manifests_p_46_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_46
    ADD CONSTRAINT manifests_p_46_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_46
    ADD CONSTRAINT manifests_p_46_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_47
    ADD CONSTRAINT manifests_p_47_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_47
    ADD CONSTRAINT manifests_p_47_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_47
    ADD CONSTRAINT manifests_p_47_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_48
    ADD CONSTRAINT manifests_p_48_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_48
    ADD CONSTRAINT manifests_p_48_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_48
    ADD CONSTRAINT manifests_p_48_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_49
    ADD CONSTRAINT manifests_p_49_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_49
    ADD CONSTRAINT manifests_p_49_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_49
    ADD CONSTRAINT manifests_p_49_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_4
    ADD CONSTRAINT manifests_p_4_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_4
    ADD CONSTRAINT manifests_p_4_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_4
    ADD CONSTRAINT manifests_p_4_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_50
    ADD CONSTRAINT manifests_p_50_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_50
    ADD CONSTRAINT manifests_p_50_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_50
    ADD CONSTRAINT manifests_p_50_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_51
    ADD CONSTRAINT manifests_p_51_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_51
    ADD CONSTRAINT manifests_p_51_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_51
    ADD CONSTRAINT manifests_p_51_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_52
    ADD CONSTRAINT manifests_p_52_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_52
    ADD CONSTRAINT manifests_p_52_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_52
    ADD CONSTRAINT manifests_p_52_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_53
    ADD CONSTRAINT manifests_p_53_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_53
    ADD CONSTRAINT manifests_p_53_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_53
    ADD CONSTRAINT manifests_p_53_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_54
    ADD CONSTRAINT manifests_p_54_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_54
    ADD CONSTRAINT manifests_p_54_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_54
    ADD CONSTRAINT manifests_p_54_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_55
    ADD CONSTRAINT manifests_p_55_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_55
    ADD CONSTRAINT manifests_p_55_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_55
    ADD CONSTRAINT manifests_p_55_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_56
    ADD CONSTRAINT manifests_p_56_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_56
    ADD CONSTRAINT manifests_p_56_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_56
    ADD CONSTRAINT manifests_p_56_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_57
    ADD CONSTRAINT manifests_p_57_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_57
    ADD CONSTRAINT manifests_p_57_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_57
    ADD CONSTRAINT manifests_p_57_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_58
    ADD CONSTRAINT manifests_p_58_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_58
    ADD CONSTRAINT manifests_p_58_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_58
    ADD CONSTRAINT manifests_p_58_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_59
    ADD CONSTRAINT manifests_p_59_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_59
    ADD CONSTRAINT manifests_p_59_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_59
    ADD CONSTRAINT manifests_p_59_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_5
    ADD CONSTRAINT manifests_p_5_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_5
    ADD CONSTRAINT manifests_p_5_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_5
    ADD CONSTRAINT manifests_p_5_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_60
    ADD CONSTRAINT manifests_p_60_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_60
    ADD CONSTRAINT manifests_p_60_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_60
    ADD CONSTRAINT manifests_p_60_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_61
    ADD CONSTRAINT manifests_p_61_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_61
    ADD CONSTRAINT manifests_p_61_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_61
    ADD CONSTRAINT manifests_p_61_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_62
    ADD CONSTRAINT manifests_p_62_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_62
    ADD CONSTRAINT manifests_p_62_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_62
    ADD CONSTRAINT manifests_p_62_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_63
    ADD CONSTRAINT manifests_p_63_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_63
    ADD CONSTRAINT manifests_p_63_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_63
    ADD CONSTRAINT manifests_p_63_top_level_namespace_id_repository_id_id_conf_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_6
    ADD CONSTRAINT manifests_p_6_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_6
    ADD CONSTRAINT manifests_p_6_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_6
    ADD CONSTRAINT manifests_p_6_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_7
    ADD CONSTRAINT manifests_p_7_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_7
    ADD CONSTRAINT manifests_p_7_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_7
    ADD CONSTRAINT manifests_p_7_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_8
    ADD CONSTRAINT manifests_p_8_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_8
    ADD CONSTRAINT manifests_p_8_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_8
    ADD CONSTRAINT manifests_p_8_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY partitions.manifests_p_9
    ADD CONSTRAINT manifests_p_9_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.manifests_p_9
    ADD CONSTRAINT manifests_p_9_top_level_namespace_id_repository_id_digest_key UNIQUE (top_level_namespace_id, repository_id, digest);

ALTER TABLE ONLY partitions.manifests_p_9
    ADD CONSTRAINT manifests_p_9_top_level_namespace_id_repository_id_id_confi_key UNIQUE (top_level_namespace_id, repository_id, id, configuration_blob_digest);

ALTER TABLE ONLY public.repository_blobs
    ADD CONSTRAINT pk_repository_blobs PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_0
    ADD CONSTRAINT repository_blobs_p_0_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY public.repository_blobs
    ADD CONSTRAINT unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_0
    ADD CONSTRAINT repository_blobs_p_0_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_10
    ADD CONSTRAINT repository_blobs_p_10_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_10
    ADD CONSTRAINT repository_blobs_p_10_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_11
    ADD CONSTRAINT repository_blobs_p_11_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_11
    ADD CONSTRAINT repository_blobs_p_11_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_12
    ADD CONSTRAINT repository_blobs_p_12_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_12
    ADD CONSTRAINT repository_blobs_p_12_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_13
    ADD CONSTRAINT repository_blobs_p_13_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_13
    ADD CONSTRAINT repository_blobs_p_13_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_14
    ADD CONSTRAINT repository_blobs_p_14_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_14
    ADD CONSTRAINT repository_blobs_p_14_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_15
    ADD CONSTRAINT repository_blobs_p_15_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_15
    ADD CONSTRAINT repository_blobs_p_15_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_16
    ADD CONSTRAINT repository_blobs_p_16_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_16
    ADD CONSTRAINT repository_blobs_p_16_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_17
    ADD CONSTRAINT repository_blobs_p_17_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_17
    ADD CONSTRAINT repository_blobs_p_17_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_18
    ADD CONSTRAINT repository_blobs_p_18_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_18
    ADD CONSTRAINT repository_blobs_p_18_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_19
    ADD CONSTRAINT repository_blobs_p_19_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_19
    ADD CONSTRAINT repository_blobs_p_19_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_1
    ADD CONSTRAINT repository_blobs_p_1_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_1
    ADD CONSTRAINT repository_blobs_p_1_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_20
    ADD CONSTRAINT repository_blobs_p_20_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_20
    ADD CONSTRAINT repository_blobs_p_20_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_21
    ADD CONSTRAINT repository_blobs_p_21_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_21
    ADD CONSTRAINT repository_blobs_p_21_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_22
    ADD CONSTRAINT repository_blobs_p_22_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_22
    ADD CONSTRAINT repository_blobs_p_22_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_23
    ADD CONSTRAINT repository_blobs_p_23_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_23
    ADD CONSTRAINT repository_blobs_p_23_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_24
    ADD CONSTRAINT repository_blobs_p_24_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_24
    ADD CONSTRAINT repository_blobs_p_24_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_25
    ADD CONSTRAINT repository_blobs_p_25_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_25
    ADD CONSTRAINT repository_blobs_p_25_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_26
    ADD CONSTRAINT repository_blobs_p_26_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_26
    ADD CONSTRAINT repository_blobs_p_26_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_27
    ADD CONSTRAINT repository_blobs_p_27_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_27
    ADD CONSTRAINT repository_blobs_p_27_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_28
    ADD CONSTRAINT repository_blobs_p_28_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_28
    ADD CONSTRAINT repository_blobs_p_28_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_29
    ADD CONSTRAINT repository_blobs_p_29_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_29
    ADD CONSTRAINT repository_blobs_p_29_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_2
    ADD CONSTRAINT repository_blobs_p_2_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_2
    ADD CONSTRAINT repository_blobs_p_2_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_30
    ADD CONSTRAINT repository_blobs_p_30_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_30
    ADD CONSTRAINT repository_blobs_p_30_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_31
    ADD CONSTRAINT repository_blobs_p_31_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_31
    ADD CONSTRAINT repository_blobs_p_31_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_32
    ADD CONSTRAINT repository_blobs_p_32_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_32
    ADD CONSTRAINT repository_blobs_p_32_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_33
    ADD CONSTRAINT repository_blobs_p_33_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_33
    ADD CONSTRAINT repository_blobs_p_33_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_34
    ADD CONSTRAINT repository_blobs_p_34_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_34
    ADD CONSTRAINT repository_blobs_p_34_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_35
    ADD CONSTRAINT repository_blobs_p_35_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_35
    ADD CONSTRAINT repository_blobs_p_35_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_36
    ADD CONSTRAINT repository_blobs_p_36_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_36
    ADD CONSTRAINT repository_blobs_p_36_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_37
    ADD CONSTRAINT repository_blobs_p_37_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_37
    ADD CONSTRAINT repository_blobs_p_37_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_38
    ADD CONSTRAINT repository_blobs_p_38_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_38
    ADD CONSTRAINT repository_blobs_p_38_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_39
    ADD CONSTRAINT repository_blobs_p_39_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_39
    ADD CONSTRAINT repository_blobs_p_39_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_3
    ADD CONSTRAINT repository_blobs_p_3_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_3
    ADD CONSTRAINT repository_blobs_p_3_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_40
    ADD CONSTRAINT repository_blobs_p_40_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_40
    ADD CONSTRAINT repository_blobs_p_40_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_41
    ADD CONSTRAINT repository_blobs_p_41_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_41
    ADD CONSTRAINT repository_blobs_p_41_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_42
    ADD CONSTRAINT repository_blobs_p_42_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_42
    ADD CONSTRAINT repository_blobs_p_42_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_43
    ADD CONSTRAINT repository_blobs_p_43_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_43
    ADD CONSTRAINT repository_blobs_p_43_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_44
    ADD CONSTRAINT repository_blobs_p_44_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_44
    ADD CONSTRAINT repository_blobs_p_44_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_45
    ADD CONSTRAINT repository_blobs_p_45_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_45
    ADD CONSTRAINT repository_blobs_p_45_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_46
    ADD CONSTRAINT repository_blobs_p_46_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_46
    ADD CONSTRAINT repository_blobs_p_46_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_47
    ADD CONSTRAINT repository_blobs_p_47_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_47
    ADD CONSTRAINT repository_blobs_p_47_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_48
    ADD CONSTRAINT repository_blobs_p_48_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_48
    ADD CONSTRAINT repository_blobs_p_48_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_49
    ADD CONSTRAINT repository_blobs_p_49_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_49
    ADD CONSTRAINT repository_blobs_p_49_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_4
    ADD CONSTRAINT repository_blobs_p_4_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_4
    ADD CONSTRAINT repository_blobs_p_4_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_50
    ADD CONSTRAINT repository_blobs_p_50_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_50
    ADD CONSTRAINT repository_blobs_p_50_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_51
    ADD CONSTRAINT repository_blobs_p_51_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_51
    ADD CONSTRAINT repository_blobs_p_51_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_52
    ADD CONSTRAINT repository_blobs_p_52_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_52
    ADD CONSTRAINT repository_blobs_p_52_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_53
    ADD CONSTRAINT repository_blobs_p_53_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_53
    ADD CONSTRAINT repository_blobs_p_53_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_54
    ADD CONSTRAINT repository_blobs_p_54_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_54
    ADD CONSTRAINT repository_blobs_p_54_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_55
    ADD CONSTRAINT repository_blobs_p_55_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_55
    ADD CONSTRAINT repository_blobs_p_55_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_56
    ADD CONSTRAINT repository_blobs_p_56_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_56
    ADD CONSTRAINT repository_blobs_p_56_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_57
    ADD CONSTRAINT repository_blobs_p_57_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_57
    ADD CONSTRAINT repository_blobs_p_57_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_58
    ADD CONSTRAINT repository_blobs_p_58_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_58
    ADD CONSTRAINT repository_blobs_p_58_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_59
    ADD CONSTRAINT repository_blobs_p_59_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_59
    ADD CONSTRAINT repository_blobs_p_59_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_5
    ADD CONSTRAINT repository_blobs_p_5_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_5
    ADD CONSTRAINT repository_blobs_p_5_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_60
    ADD CONSTRAINT repository_blobs_p_60_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_60
    ADD CONSTRAINT repository_blobs_p_60_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_61
    ADD CONSTRAINT repository_blobs_p_61_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_61
    ADD CONSTRAINT repository_blobs_p_61_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_62
    ADD CONSTRAINT repository_blobs_p_62_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_62
    ADD CONSTRAINT repository_blobs_p_62_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_63
    ADD CONSTRAINT repository_blobs_p_63_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_63
    ADD CONSTRAINT repository_blobs_p_63_top_level_namespace_id_repository_id__key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_6
    ADD CONSTRAINT repository_blobs_p_6_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_6
    ADD CONSTRAINT repository_blobs_p_6_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_7
    ADD CONSTRAINT repository_blobs_p_7_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_7
    ADD CONSTRAINT repository_blobs_p_7_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_8
    ADD CONSTRAINT repository_blobs_p_8_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_8
    ADD CONSTRAINT repository_blobs_p_8_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY partitions.repository_blobs_p_9
    ADD CONSTRAINT repository_blobs_p_9_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.repository_blobs_p_9
    ADD CONSTRAINT repository_blobs_p_9_top_level_namespace_id_repository_id_b_key UNIQUE (top_level_namespace_id, repository_id, blob_digest);

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT pk_tags PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_0
    ADD CONSTRAINT tags_p_0_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT unique_tags_top_level_namespace_id_and_repository_id_and_name UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_0
    ADD CONSTRAINT tags_p_0_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_10
    ADD CONSTRAINT tags_p_10_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_10
    ADD CONSTRAINT tags_p_10_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_11
    ADD CONSTRAINT tags_p_11_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_11
    ADD CONSTRAINT tags_p_11_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_12
    ADD CONSTRAINT tags_p_12_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_12
    ADD CONSTRAINT tags_p_12_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_13
    ADD CONSTRAINT tags_p_13_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_13
    ADD CONSTRAINT tags_p_13_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_14
    ADD CONSTRAINT tags_p_14_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_14
    ADD CONSTRAINT tags_p_14_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_15
    ADD CONSTRAINT tags_p_15_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_15
    ADD CONSTRAINT tags_p_15_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_16
    ADD CONSTRAINT tags_p_16_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_16
    ADD CONSTRAINT tags_p_16_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_17
    ADD CONSTRAINT tags_p_17_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_17
    ADD CONSTRAINT tags_p_17_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_18
    ADD CONSTRAINT tags_p_18_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_18
    ADD CONSTRAINT tags_p_18_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_19
    ADD CONSTRAINT tags_p_19_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_19
    ADD CONSTRAINT tags_p_19_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_1
    ADD CONSTRAINT tags_p_1_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_1
    ADD CONSTRAINT tags_p_1_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_20
    ADD CONSTRAINT tags_p_20_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_20
    ADD CONSTRAINT tags_p_20_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_21
    ADD CONSTRAINT tags_p_21_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_21
    ADD CONSTRAINT tags_p_21_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_22
    ADD CONSTRAINT tags_p_22_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_22
    ADD CONSTRAINT tags_p_22_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_23
    ADD CONSTRAINT tags_p_23_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_23
    ADD CONSTRAINT tags_p_23_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_24
    ADD CONSTRAINT tags_p_24_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_24
    ADD CONSTRAINT tags_p_24_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_25
    ADD CONSTRAINT tags_p_25_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_25
    ADD CONSTRAINT tags_p_25_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_26
    ADD CONSTRAINT tags_p_26_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_26
    ADD CONSTRAINT tags_p_26_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_27
    ADD CONSTRAINT tags_p_27_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_27
    ADD CONSTRAINT tags_p_27_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_28
    ADD CONSTRAINT tags_p_28_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_28
    ADD CONSTRAINT tags_p_28_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_29
    ADD CONSTRAINT tags_p_29_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_29
    ADD CONSTRAINT tags_p_29_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_2
    ADD CONSTRAINT tags_p_2_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_2
    ADD CONSTRAINT tags_p_2_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_30
    ADD CONSTRAINT tags_p_30_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_30
    ADD CONSTRAINT tags_p_30_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_31
    ADD CONSTRAINT tags_p_31_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_31
    ADD CONSTRAINT tags_p_31_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_32
    ADD CONSTRAINT tags_p_32_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_32
    ADD CONSTRAINT tags_p_32_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_33
    ADD CONSTRAINT tags_p_33_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_33
    ADD CONSTRAINT tags_p_33_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_34
    ADD CONSTRAINT tags_p_34_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_34
    ADD CONSTRAINT tags_p_34_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_35
    ADD CONSTRAINT tags_p_35_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_35
    ADD CONSTRAINT tags_p_35_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_36
    ADD CONSTRAINT tags_p_36_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_36
    ADD CONSTRAINT tags_p_36_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_37
    ADD CONSTRAINT tags_p_37_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_37
    ADD CONSTRAINT tags_p_37_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_38
    ADD CONSTRAINT tags_p_38_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_38
    ADD CONSTRAINT tags_p_38_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_39
    ADD CONSTRAINT tags_p_39_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_39
    ADD CONSTRAINT tags_p_39_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_3
    ADD CONSTRAINT tags_p_3_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_3
    ADD CONSTRAINT tags_p_3_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_40
    ADD CONSTRAINT tags_p_40_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_40
    ADD CONSTRAINT tags_p_40_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_41
    ADD CONSTRAINT tags_p_41_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_41
    ADD CONSTRAINT tags_p_41_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_42
    ADD CONSTRAINT tags_p_42_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_42
    ADD CONSTRAINT tags_p_42_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_43
    ADD CONSTRAINT tags_p_43_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_43
    ADD CONSTRAINT tags_p_43_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_44
    ADD CONSTRAINT tags_p_44_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_44
    ADD CONSTRAINT tags_p_44_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_45
    ADD CONSTRAINT tags_p_45_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_45
    ADD CONSTRAINT tags_p_45_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_46
    ADD CONSTRAINT tags_p_46_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_46
    ADD CONSTRAINT tags_p_46_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_47
    ADD CONSTRAINT tags_p_47_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_47
    ADD CONSTRAINT tags_p_47_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_48
    ADD CONSTRAINT tags_p_48_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_48
    ADD CONSTRAINT tags_p_48_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_49
    ADD CONSTRAINT tags_p_49_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_49
    ADD CONSTRAINT tags_p_49_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_4
    ADD CONSTRAINT tags_p_4_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_4
    ADD CONSTRAINT tags_p_4_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_50
    ADD CONSTRAINT tags_p_50_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_50
    ADD CONSTRAINT tags_p_50_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_51
    ADD CONSTRAINT tags_p_51_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_51
    ADD CONSTRAINT tags_p_51_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_52
    ADD CONSTRAINT tags_p_52_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_52
    ADD CONSTRAINT tags_p_52_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_53
    ADD CONSTRAINT tags_p_53_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_53
    ADD CONSTRAINT tags_p_53_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_54
    ADD CONSTRAINT tags_p_54_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_54
    ADD CONSTRAINT tags_p_54_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_55
    ADD CONSTRAINT tags_p_55_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_55
    ADD CONSTRAINT tags_p_55_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_56
    ADD CONSTRAINT tags_p_56_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_56
    ADD CONSTRAINT tags_p_56_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_57
    ADD CONSTRAINT tags_p_57_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_57
    ADD CONSTRAINT tags_p_57_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_58
    ADD CONSTRAINT tags_p_58_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_58
    ADD CONSTRAINT tags_p_58_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_59
    ADD CONSTRAINT tags_p_59_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_59
    ADD CONSTRAINT tags_p_59_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_5
    ADD CONSTRAINT tags_p_5_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_5
    ADD CONSTRAINT tags_p_5_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_60
    ADD CONSTRAINT tags_p_60_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_60
    ADD CONSTRAINT tags_p_60_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_61
    ADD CONSTRAINT tags_p_61_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_61
    ADD CONSTRAINT tags_p_61_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_62
    ADD CONSTRAINT tags_p_62_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_62
    ADD CONSTRAINT tags_p_62_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_63
    ADD CONSTRAINT tags_p_63_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_63
    ADD CONSTRAINT tags_p_63_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_6
    ADD CONSTRAINT tags_p_6_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_6
    ADD CONSTRAINT tags_p_6_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_7
    ADD CONSTRAINT tags_p_7_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_7
    ADD CONSTRAINT tags_p_7_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_8
    ADD CONSTRAINT tags_p_8_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_8
    ADD CONSTRAINT tags_p_8_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY partitions.tags_p_9
    ADD CONSTRAINT tags_p_9_pkey PRIMARY KEY (top_level_namespace_id, repository_id, id);

ALTER TABLE ONLY partitions.tags_p_9
    ADD CONSTRAINT tags_p_9_top_level_namespace_id_repository_id_name_key UNIQUE (top_level_namespace_id, repository_id, name);

ALTER TABLE ONLY public.gc_blob_review_queue
    ADD CONSTRAINT pk_gc_blob_review_queue PRIMARY KEY (digest);

ALTER TABLE ONLY public.gc_manifest_review_queue
    ADD CONSTRAINT pk_gc_manifest_review_queue PRIMARY KEY (top_level_namespace_id, repository_id, manifest_id);

ALTER TABLE ONLY public.gc_review_after_defaults
    ADD CONSTRAINT pk_gc_review_after_defaults PRIMARY KEY (event);

ALTER TABLE ONLY public.gc_tmp_blobs_manifests
    ADD CONSTRAINT pk_gc_tmp_blobs_manifests PRIMARY KEY (digest);

ALTER TABLE ONLY public.media_types
    ADD CONSTRAINT pk_media_types PRIMARY KEY (id);

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT pk_repositories PRIMARY KEY (top_level_namespace_id, id);

ALTER TABLE ONLY public.top_level_namespaces
    ADD CONSTRAINT pk_top_level_namespaces PRIMARY KEY (id);

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.media_types
    ADD CONSTRAINT unique_media_types_type UNIQUE (media_type);

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT unique_repositories_path UNIQUE (path);

ALTER TABLE ONLY public.top_level_namespaces
    ADD CONSTRAINT unique_top_level_namespaces_name UNIQUE (name);

CREATE INDEX index_blobs_on_media_type_id ON ONLY public.blobs USING btree (media_type_id);

CREATE INDEX blobs_p_0_media_type_id_idx ON partitions.blobs_p_0 USING btree (media_type_id);

CREATE INDEX blobs_p_10_media_type_id_idx ON partitions.blobs_p_10 USING btree (media_type_id);

CREATE INDEX blobs_p_11_media_type_id_idx ON partitions.blobs_p_11 USING btree (media_type_id);

CREATE INDEX blobs_p_12_media_type_id_idx ON partitions.blobs_p_12 USING btree (media_type_id);

CREATE INDEX blobs_p_13_media_type_id_idx ON partitions.blobs_p_13 USING btree (media_type_id);

CREATE INDEX blobs_p_14_media_type_id_idx ON partitions.blobs_p_14 USING btree (media_type_id);

CREATE INDEX blobs_p_15_media_type_id_idx ON partitions.blobs_p_15 USING btree (media_type_id);

CREATE INDEX blobs_p_16_media_type_id_idx ON partitions.blobs_p_16 USING btree (media_type_id);

CREATE INDEX blobs_p_17_media_type_id_idx ON partitions.blobs_p_17 USING btree (media_type_id);

CREATE INDEX blobs_p_18_media_type_id_idx ON partitions.blobs_p_18 USING btree (media_type_id);

CREATE INDEX blobs_p_19_media_type_id_idx ON partitions.blobs_p_19 USING btree (media_type_id);

CREATE INDEX blobs_p_1_media_type_id_idx ON partitions.blobs_p_1 USING btree (media_type_id);

CREATE INDEX blobs_p_20_media_type_id_idx ON partitions.blobs_p_20 USING btree (media_type_id);

CREATE INDEX blobs_p_21_media_type_id_idx ON partitions.blobs_p_21 USING btree (media_type_id);

CREATE INDEX blobs_p_22_media_type_id_idx ON partitions.blobs_p_22 USING btree (media_type_id);

CREATE INDEX blobs_p_23_media_type_id_idx ON partitions.blobs_p_23 USING btree (media_type_id);

CREATE INDEX blobs_p_24_media_type_id_idx ON partitions.blobs_p_24 USING btree (media_type_id);

CREATE INDEX blobs_p_25_media_type_id_idx ON partitions.blobs_p_25 USING btree (media_type_id);

CREATE INDEX blobs_p_26_media_type_id_idx ON partitions.blobs_p_26 USING btree (media_type_id);

CREATE INDEX blobs_p_27_media_type_id_idx ON partitions.blobs_p_27 USING btree (media_type_id);

CREATE INDEX blobs_p_28_media_type_id_idx ON partitions.blobs_p_28 USING btree (media_type_id);

CREATE INDEX blobs_p_29_media_type_id_idx ON partitions.blobs_p_29 USING btree (media_type_id);

CREATE INDEX blobs_p_2_media_type_id_idx ON partitions.blobs_p_2 USING btree (media_type_id);

CREATE INDEX blobs_p_30_media_type_id_idx ON partitions.blobs_p_30 USING btree (media_type_id);

CREATE INDEX blobs_p_31_media_type_id_idx ON partitions.blobs_p_31 USING btree (media_type_id);

CREATE INDEX blobs_p_32_media_type_id_idx ON partitions.blobs_p_32 USING btree (media_type_id);

CREATE INDEX blobs_p_33_media_type_id_idx ON partitions.blobs_p_33 USING btree (media_type_id);

CREATE INDEX blobs_p_34_media_type_id_idx ON partitions.blobs_p_34 USING btree (media_type_id);

CREATE INDEX blobs_p_35_media_type_id_idx ON partitions.blobs_p_35 USING btree (media_type_id);

CREATE INDEX blobs_p_36_media_type_id_idx ON partitions.blobs_p_36 USING btree (media_type_id);

CREATE INDEX blobs_p_37_media_type_id_idx ON partitions.blobs_p_37 USING btree (media_type_id);

CREATE INDEX blobs_p_38_media_type_id_idx ON partitions.blobs_p_38 USING btree (media_type_id);

CREATE INDEX blobs_p_39_media_type_id_idx ON partitions.blobs_p_39 USING btree (media_type_id);

CREATE INDEX blobs_p_3_media_type_id_idx ON partitions.blobs_p_3 USING btree (media_type_id);

CREATE INDEX blobs_p_40_media_type_id_idx ON partitions.blobs_p_40 USING btree (media_type_id);

CREATE INDEX blobs_p_41_media_type_id_idx ON partitions.blobs_p_41 USING btree (media_type_id);

CREATE INDEX blobs_p_42_media_type_id_idx ON partitions.blobs_p_42 USING btree (media_type_id);

CREATE INDEX blobs_p_43_media_type_id_idx ON partitions.blobs_p_43 USING btree (media_type_id);

CREATE INDEX blobs_p_44_media_type_id_idx ON partitions.blobs_p_44 USING btree (media_type_id);

CREATE INDEX blobs_p_45_media_type_id_idx ON partitions.blobs_p_45 USING btree (media_type_id);

CREATE INDEX blobs_p_46_media_type_id_idx ON partitions.blobs_p_46 USING btree (media_type_id);

CREATE INDEX blobs_p_47_media_type_id_idx ON partitions.blobs_p_47 USING btree (media_type_id);

CREATE INDEX blobs_p_48_media_type_id_idx ON partitions.blobs_p_48 USING btree (media_type_id);

CREATE INDEX blobs_p_49_media_type_id_idx ON partitions.blobs_p_49 USING btree (media_type_id);

CREATE INDEX blobs_p_4_media_type_id_idx ON partitions.blobs_p_4 USING btree (media_type_id);

CREATE INDEX blobs_p_50_media_type_id_idx ON partitions.blobs_p_50 USING btree (media_type_id);

CREATE INDEX blobs_p_51_media_type_id_idx ON partitions.blobs_p_51 USING btree (media_type_id);

CREATE INDEX blobs_p_52_media_type_id_idx ON partitions.blobs_p_52 USING btree (media_type_id);

CREATE INDEX blobs_p_53_media_type_id_idx ON partitions.blobs_p_53 USING btree (media_type_id);

CREATE INDEX blobs_p_54_media_type_id_idx ON partitions.blobs_p_54 USING btree (media_type_id);

CREATE INDEX blobs_p_55_media_type_id_idx ON partitions.blobs_p_55 USING btree (media_type_id);

CREATE INDEX blobs_p_56_media_type_id_idx ON partitions.blobs_p_56 USING btree (media_type_id);

CREATE INDEX blobs_p_57_media_type_id_idx ON partitions.blobs_p_57 USING btree (media_type_id);

CREATE INDEX blobs_p_58_media_type_id_idx ON partitions.blobs_p_58 USING btree (media_type_id);

CREATE INDEX blobs_p_59_media_type_id_idx ON partitions.blobs_p_59 USING btree (media_type_id);

CREATE INDEX blobs_p_5_media_type_id_idx ON partitions.blobs_p_5 USING btree (media_type_id);

CREATE INDEX blobs_p_60_media_type_id_idx ON partitions.blobs_p_60 USING btree (media_type_id);

CREATE INDEX blobs_p_61_media_type_id_idx ON partitions.blobs_p_61 USING btree (media_type_id);

CREATE INDEX blobs_p_62_media_type_id_idx ON partitions.blobs_p_62 USING btree (media_type_id);

CREATE INDEX blobs_p_63_media_type_id_idx ON partitions.blobs_p_63 USING btree (media_type_id);

CREATE INDEX blobs_p_6_media_type_id_idx ON partitions.blobs_p_6 USING btree (media_type_id);

CREATE INDEX blobs_p_7_media_type_id_idx ON partitions.blobs_p_7 USING btree (media_type_id);

CREATE INDEX blobs_p_8_media_type_id_idx ON partitions.blobs_p_8 USING btree (media_type_id);

CREATE INDEX blobs_p_9_media_type_id_idx ON partitions.blobs_p_9 USING btree (media_type_id);

CREATE INDEX index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ON ONLY public.gc_blobs_configurations USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_0_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_0 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_10_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_10 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_11_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_11 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_12_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_12 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_13_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_13 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_14_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_14 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_15_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_15 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_16_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_16 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_17_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_17 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_18_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_18 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_19_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_19 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_1_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_1 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_20_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_20 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_21_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_21 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_22_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_22 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_23_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_23 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_24_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_24 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_25_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_25 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_26_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_26 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_27_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_27 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_28_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_28 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_29_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_29 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_2_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_2 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_30_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_30 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_31_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_31 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_32_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_32 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_33_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_33 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_34_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_34 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_35_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_35 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_36_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_36 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_37_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_37 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_38_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_38 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_39_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_39 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_3_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_3 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_40_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_40 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_41_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_41 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_42_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_42 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_43_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_43 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_44_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_44 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_45_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_45 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_46_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_46 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_47_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_47 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_48_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_48 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_49_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_49 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_4_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_4 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_50_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_50 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_51_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_51 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_52_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_52 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_53_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_53 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_54_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_54 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_55_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_55 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_56_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_56 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_57_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_57 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_58_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_58 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_59_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_59 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_5_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_5 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_60_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_60 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_61_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_61 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_62_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_62 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_63_top_level_namespace_id_reposit_idx ON partitions.gc_blobs_configurations_p_63 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_6_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_6 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_7_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_7 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_8_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_8 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX gc_blobs_configurations_p_9_top_level_namespace_id_reposito_idx ON partitions.gc_blobs_configurations_p_9 USING btree (top_level_namespace_id, repository_id, manifest_id, digest);

CREATE INDEX index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ON ONLY public.gc_blobs_layers USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_0_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_0 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_10_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_10 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_11_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_11 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_12_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_12 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_13_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_13 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_14_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_14 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_15_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_15 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_16_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_16 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_17_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_17 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_18_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_18 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_19_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_19 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_1_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_1 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_20_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_20 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_21_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_21 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_22_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_22 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_23_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_23 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_24_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_24 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_25_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_25 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_26_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_26 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_27_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_27 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_28_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_28 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_29_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_29 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_2_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_2 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_30_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_30 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_31_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_31 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_32_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_32 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_33_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_33 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_34_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_34 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_35_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_35 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_36_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_36 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_37_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_37 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_38_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_38 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_39_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_39 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_3_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_3 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_40_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_40 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_41_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_41 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_42_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_42 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_43_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_43 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_44_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_44 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_45_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_45 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_46_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_46 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_47_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_47 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_48_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_48 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_49_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_49 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_4_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_4 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_50_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_50 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_51_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_51 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_52_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_52 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_53_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_53 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_54_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_54 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_55_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_55 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_56_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_56 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_57_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_57 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_58_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_58 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_59_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_59 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_5_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_5 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_60_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_60 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_61_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_61 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_62_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_62 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_63_top_level_namespace_id_repository_id_l_idx ON partitions.gc_blobs_layers_p_63 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_6_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_6 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_7_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_7 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_8_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_8 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX gc_blobs_layers_p_9_top_level_namespace_id_repository_id_la_idx ON partitions.gc_blobs_layers_p_9 USING btree (top_level_namespace_id, repository_id, layer_id, digest);

CREATE INDEX index_layers_on_top_level_namespace_id_and_digest_and_size ON ONLY public.layers USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_0_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_0 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_10_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_10 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_11_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_11 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_12_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_12 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_13_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_13 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_14_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_14 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_15_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_15 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_16_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_16 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_17_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_17 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_18_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_18 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_19_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_19 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_1_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_1 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_20_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_20 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_21_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_21 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_22_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_22 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_23_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_23 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_24_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_24 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_25_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_25 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_26_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_26 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_27_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_27 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_28_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_28 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_29_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_29 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_2_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_2 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_30_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_30 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_31_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_31 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_32_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_32 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_33_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_33 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_34_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_34 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_35_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_35 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_36_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_36 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_37_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_37 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_38_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_38 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_39_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_39 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_3_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_3 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_40_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_40 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_41_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_41 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_42_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_42 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_43_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_43 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_44_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_44 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_45_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_45 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_46_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_46 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_47_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_47 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_48_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_48 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_49_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_49 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_4_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_4 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_50_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_50 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_51_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_51 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_52_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_52 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_53_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_53 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_54_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_54 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_55_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_55 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_56_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_56 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_57_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_57 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_58_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_58 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_59_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_59 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_5_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_5 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_60_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_60 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_61_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_61 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_62_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_62 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_63_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_63 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_6_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_6 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_7_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_7 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_8_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_8 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_layers_p_9_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_9 USING btree (top_level_namespace_id, digest, size);

CREATE INDEX index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ON ONLY public.tags USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_0 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_10_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_10 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_11_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_11 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_12_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_12 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_13_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_13 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_14_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_14 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_15_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_15 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_16_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_16 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_17_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_17 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_18_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_18 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_19_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_19 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_1 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_20_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_20 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_21_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_21 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_22_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_22 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_23_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_23 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_24_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_24 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_25_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_25 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_26_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_26 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_27_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_27 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_28_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_28 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_29_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_29 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_2 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_30_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_30 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_31_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_31 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_32_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_32 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_33_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_33 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_34_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_34 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_35_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_35 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_36_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_36 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_37_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_37 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_38_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_38 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_39_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_39 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_3 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_40_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_40 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_41_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_41 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_42_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_42 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_43_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_43 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_44_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_44 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_45_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_45 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_46_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_46 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_47_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_47 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_48_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_48 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_49_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_49 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_4_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_4 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_50_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_50 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_51_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_51 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_52_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_52 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_53_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_53 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_54_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_54 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_55_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_55 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_56_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_56 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_57_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_57 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_58_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_58 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_59_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_59 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_5_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_5 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_60_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_60 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_61_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_61 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_62_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_62 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_63_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_63 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_6_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_6 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_7_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_7 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_8_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_8 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_tags_p_9_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_9 USING btree (top_level_namespace_id, repository_id, manifest_id, name);

CREATE INDEX index_layers_on_digest ON ONLY public.layers USING btree (digest);

CREATE INDEX layers_p_0_digest_idx ON partitions.layers_p_0 USING btree (digest);

CREATE INDEX index_layers_on_media_type_id ON ONLY public.layers USING btree (media_type_id);

CREATE INDEX layers_p_0_media_type_id_idx ON partitions.layers_p_0 USING btree (media_type_id);

CREATE INDEX layers_p_10_digest_idx ON partitions.layers_p_10 USING btree (digest);

CREATE INDEX layers_p_10_media_type_id_idx ON partitions.layers_p_10 USING btree (media_type_id);

CREATE INDEX layers_p_11_digest_idx ON partitions.layers_p_11 USING btree (digest);

CREATE INDEX layers_p_11_media_type_id_idx ON partitions.layers_p_11 USING btree (media_type_id);

CREATE INDEX layers_p_12_digest_idx ON partitions.layers_p_12 USING btree (digest);

CREATE INDEX layers_p_12_media_type_id_idx ON partitions.layers_p_12 USING btree (media_type_id);

CREATE INDEX layers_p_13_digest_idx ON partitions.layers_p_13 USING btree (digest);

CREATE INDEX layers_p_13_media_type_id_idx ON partitions.layers_p_13 USING btree (media_type_id);

CREATE INDEX layers_p_14_digest_idx ON partitions.layers_p_14 USING btree (digest);

CREATE INDEX layers_p_14_media_type_id_idx ON partitions.layers_p_14 USING btree (media_type_id);

CREATE INDEX layers_p_15_digest_idx ON partitions.layers_p_15 USING btree (digest);

CREATE INDEX layers_p_15_media_type_id_idx ON partitions.layers_p_15 USING btree (media_type_id);

CREATE INDEX layers_p_16_digest_idx ON partitions.layers_p_16 USING btree (digest);

CREATE INDEX layers_p_16_media_type_id_idx ON partitions.layers_p_16 USING btree (media_type_id);

CREATE INDEX layers_p_17_digest_idx ON partitions.layers_p_17 USING btree (digest);

CREATE INDEX layers_p_17_media_type_id_idx ON partitions.layers_p_17 USING btree (media_type_id);

CREATE INDEX layers_p_18_digest_idx ON partitions.layers_p_18 USING btree (digest);

CREATE INDEX layers_p_18_media_type_id_idx ON partitions.layers_p_18 USING btree (media_type_id);

CREATE INDEX layers_p_19_digest_idx ON partitions.layers_p_19 USING btree (digest);

CREATE INDEX layers_p_19_media_type_id_idx ON partitions.layers_p_19 USING btree (media_type_id);

CREATE INDEX layers_p_1_digest_idx ON partitions.layers_p_1 USING btree (digest);

CREATE INDEX layers_p_1_media_type_id_idx ON partitions.layers_p_1 USING btree (media_type_id);

CREATE INDEX layers_p_20_digest_idx ON partitions.layers_p_20 USING btree (digest);

CREATE INDEX layers_p_20_media_type_id_idx ON partitions.layers_p_20 USING btree (media_type_id);

CREATE INDEX layers_p_21_digest_idx ON partitions.layers_p_21 USING btree (digest);

CREATE INDEX layers_p_21_media_type_id_idx ON partitions.layers_p_21 USING btree (media_type_id);

CREATE INDEX layers_p_22_digest_idx ON partitions.layers_p_22 USING btree (digest);

CREATE INDEX layers_p_22_media_type_id_idx ON partitions.layers_p_22 USING btree (media_type_id);

CREATE INDEX layers_p_23_digest_idx ON partitions.layers_p_23 USING btree (digest);

CREATE INDEX layers_p_23_media_type_id_idx ON partitions.layers_p_23 USING btree (media_type_id);

CREATE INDEX layers_p_24_digest_idx ON partitions.layers_p_24 USING btree (digest);

CREATE INDEX layers_p_24_media_type_id_idx ON partitions.layers_p_24 USING btree (media_type_id);

CREATE INDEX layers_p_25_digest_idx ON partitions.layers_p_25 USING btree (digest);

CREATE INDEX layers_p_25_media_type_id_idx ON partitions.layers_p_25 USING btree (media_type_id);

CREATE INDEX layers_p_26_digest_idx ON partitions.layers_p_26 USING btree (digest);

CREATE INDEX layers_p_26_media_type_id_idx ON partitions.layers_p_26 USING btree (media_type_id);

CREATE INDEX layers_p_27_digest_idx ON partitions.layers_p_27 USING btree (digest);

CREATE INDEX layers_p_27_media_type_id_idx ON partitions.layers_p_27 USING btree (media_type_id);

CREATE INDEX layers_p_28_digest_idx ON partitions.layers_p_28 USING btree (digest);

CREATE INDEX layers_p_28_media_type_id_idx ON partitions.layers_p_28 USING btree (media_type_id);

CREATE INDEX layers_p_29_digest_idx ON partitions.layers_p_29 USING btree (digest);

CREATE INDEX layers_p_29_media_type_id_idx ON partitions.layers_p_29 USING btree (media_type_id);

CREATE INDEX layers_p_2_digest_idx ON partitions.layers_p_2 USING btree (digest);

CREATE INDEX layers_p_2_media_type_id_idx ON partitions.layers_p_2 USING btree (media_type_id);

CREATE INDEX layers_p_30_digest_idx ON partitions.layers_p_30 USING btree (digest);

CREATE INDEX layers_p_30_media_type_id_idx ON partitions.layers_p_30 USING btree (media_type_id);

CREATE INDEX layers_p_31_digest_idx ON partitions.layers_p_31 USING btree (digest);

CREATE INDEX layers_p_31_media_type_id_idx ON partitions.layers_p_31 USING btree (media_type_id);

CREATE INDEX layers_p_32_digest_idx ON partitions.layers_p_32 USING btree (digest);

CREATE INDEX layers_p_32_media_type_id_idx ON partitions.layers_p_32 USING btree (media_type_id);

CREATE INDEX layers_p_33_digest_idx ON partitions.layers_p_33 USING btree (digest);

CREATE INDEX layers_p_33_media_type_id_idx ON partitions.layers_p_33 USING btree (media_type_id);

CREATE INDEX layers_p_34_digest_idx ON partitions.layers_p_34 USING btree (digest);

CREATE INDEX layers_p_34_media_type_id_idx ON partitions.layers_p_34 USING btree (media_type_id);

CREATE INDEX layers_p_35_digest_idx ON partitions.layers_p_35 USING btree (digest);

CREATE INDEX layers_p_35_media_type_id_idx ON partitions.layers_p_35 USING btree (media_type_id);

CREATE INDEX layers_p_36_digest_idx ON partitions.layers_p_36 USING btree (digest);

CREATE INDEX layers_p_36_media_type_id_idx ON partitions.layers_p_36 USING btree (media_type_id);

CREATE INDEX layers_p_37_digest_idx ON partitions.layers_p_37 USING btree (digest);

CREATE INDEX layers_p_37_media_type_id_idx ON partitions.layers_p_37 USING btree (media_type_id);

CREATE INDEX layers_p_38_digest_idx ON partitions.layers_p_38 USING btree (digest);

CREATE INDEX layers_p_38_media_type_id_idx ON partitions.layers_p_38 USING btree (media_type_id);

CREATE INDEX layers_p_39_digest_idx ON partitions.layers_p_39 USING btree (digest);

CREATE INDEX layers_p_39_media_type_id_idx ON partitions.layers_p_39 USING btree (media_type_id);

CREATE INDEX layers_p_3_digest_idx ON partitions.layers_p_3 USING btree (digest);

CREATE INDEX layers_p_3_media_type_id_idx ON partitions.layers_p_3 USING btree (media_type_id);

CREATE INDEX layers_p_40_digest_idx ON partitions.layers_p_40 USING btree (digest);

CREATE INDEX layers_p_40_media_type_id_idx ON partitions.layers_p_40 USING btree (media_type_id);

CREATE INDEX layers_p_41_digest_idx ON partitions.layers_p_41 USING btree (digest);

CREATE INDEX layers_p_41_media_type_id_idx ON partitions.layers_p_41 USING btree (media_type_id);

CREATE INDEX layers_p_42_digest_idx ON partitions.layers_p_42 USING btree (digest);

CREATE INDEX layers_p_42_media_type_id_idx ON partitions.layers_p_42 USING btree (media_type_id);

CREATE INDEX layers_p_43_digest_idx ON partitions.layers_p_43 USING btree (digest);

CREATE INDEX layers_p_43_media_type_id_idx ON partitions.layers_p_43 USING btree (media_type_id);

CREATE INDEX layers_p_44_digest_idx ON partitions.layers_p_44 USING btree (digest);

CREATE INDEX layers_p_44_media_type_id_idx ON partitions.layers_p_44 USING btree (media_type_id);

CREATE INDEX layers_p_45_digest_idx ON partitions.layers_p_45 USING btree (digest);

CREATE INDEX layers_p_45_media_type_id_idx ON partitions.layers_p_45 USING btree (media_type_id);

CREATE INDEX layers_p_46_digest_idx ON partitions.layers_p_46 USING btree (digest);

CREATE INDEX layers_p_46_media_type_id_idx ON partitions.layers_p_46 USING btree (media_type_id);

CREATE INDEX layers_p_47_digest_idx ON partitions.layers_p_47 USING btree (digest);

CREATE INDEX layers_p_47_media_type_id_idx ON partitions.layers_p_47 USING btree (media_type_id);

CREATE INDEX layers_p_48_digest_idx ON partitions.layers_p_48 USING btree (digest);

CREATE INDEX layers_p_48_media_type_id_idx ON partitions.layers_p_48 USING btree (media_type_id);

CREATE INDEX layers_p_49_digest_idx ON partitions.layers_p_49 USING btree (digest);

CREATE INDEX layers_p_49_media_type_id_idx ON partitions.layers_p_49 USING btree (media_type_id);

CREATE INDEX layers_p_4_digest_idx ON partitions.layers_p_4 USING btree (digest);

CREATE INDEX layers_p_4_media_type_id_idx ON partitions.layers_p_4 USING btree (media_type_id);

CREATE INDEX layers_p_50_digest_idx ON partitions.layers_p_50 USING btree (digest);

CREATE INDEX layers_p_50_media_type_id_idx ON partitions.layers_p_50 USING btree (media_type_id);

CREATE INDEX layers_p_51_digest_idx ON partitions.layers_p_51 USING btree (digest);

CREATE INDEX layers_p_51_media_type_id_idx ON partitions.layers_p_51 USING btree (media_type_id);

CREATE INDEX layers_p_52_digest_idx ON partitions.layers_p_52 USING btree (digest);

CREATE INDEX layers_p_52_media_type_id_idx ON partitions.layers_p_52 USING btree (media_type_id);

CREATE INDEX layers_p_53_digest_idx ON partitions.layers_p_53 USING btree (digest);

CREATE INDEX layers_p_53_media_type_id_idx ON partitions.layers_p_53 USING btree (media_type_id);

CREATE INDEX layers_p_54_digest_idx ON partitions.layers_p_54 USING btree (digest);

CREATE INDEX layers_p_54_media_type_id_idx ON partitions.layers_p_54 USING btree (media_type_id);

CREATE INDEX layers_p_55_digest_idx ON partitions.layers_p_55 USING btree (digest);

CREATE INDEX layers_p_55_media_type_id_idx ON partitions.layers_p_55 USING btree (media_type_id);

CREATE INDEX layers_p_56_digest_idx ON partitions.layers_p_56 USING btree (digest);

CREATE INDEX layers_p_56_media_type_id_idx ON partitions.layers_p_56 USING btree (media_type_id);

CREATE INDEX layers_p_57_digest_idx ON partitions.layers_p_57 USING btree (digest);

CREATE INDEX layers_p_57_media_type_id_idx ON partitions.layers_p_57 USING btree (media_type_id);

CREATE INDEX layers_p_58_digest_idx ON partitions.layers_p_58 USING btree (digest);

CREATE INDEX layers_p_58_media_type_id_idx ON partitions.layers_p_58 USING btree (media_type_id);

CREATE INDEX layers_p_59_digest_idx ON partitions.layers_p_59 USING btree (digest);

CREATE INDEX layers_p_59_media_type_id_idx ON partitions.layers_p_59 USING btree (media_type_id);

CREATE INDEX layers_p_5_digest_idx ON partitions.layers_p_5 USING btree (digest);

CREATE INDEX layers_p_5_media_type_id_idx ON partitions.layers_p_5 USING btree (media_type_id);

CREATE INDEX layers_p_60_digest_idx ON partitions.layers_p_60 USING btree (digest);

CREATE INDEX layers_p_60_media_type_id_idx ON partitions.layers_p_60 USING btree (media_type_id);

CREATE INDEX layers_p_61_digest_idx ON partitions.layers_p_61 USING btree (digest);

CREATE INDEX layers_p_61_media_type_id_idx ON partitions.layers_p_61 USING btree (media_type_id);

CREATE INDEX layers_p_62_digest_idx ON partitions.layers_p_62 USING btree (digest);

CREATE INDEX layers_p_62_media_type_id_idx ON partitions.layers_p_62 USING btree (media_type_id);

CREATE INDEX layers_p_63_digest_idx ON partitions.layers_p_63 USING btree (digest);

CREATE INDEX layers_p_63_media_type_id_idx ON partitions.layers_p_63 USING btree (media_type_id);

CREATE INDEX layers_p_6_digest_idx ON partitions.layers_p_6 USING btree (digest);

CREATE INDEX layers_p_6_media_type_id_idx ON partitions.layers_p_6 USING btree (media_type_id);

CREATE INDEX layers_p_7_digest_idx ON partitions.layers_p_7 USING btree (digest);

CREATE INDEX layers_p_7_media_type_id_idx ON partitions.layers_p_7 USING btree (media_type_id);

CREATE INDEX layers_p_8_digest_idx ON partitions.layers_p_8 USING btree (digest);

CREATE INDEX layers_p_8_media_type_id_idx ON partitions.layers_p_8 USING btree (media_type_id);

CREATE INDEX layers_p_9_digest_idx ON partitions.layers_p_9 USING btree (digest);

CREATE INDEX layers_p_9_media_type_id_idx ON partitions.layers_p_9 USING btree (media_type_id);

CREATE INDEX index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ON ONLY public.manifest_references USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_0_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_0 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_10_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_10 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_11_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_11 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_12_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_12 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_13_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_13 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_14_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_14 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_15_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_15 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_16_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_16 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_17_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_17 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_18_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_18 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_19_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_19 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_1_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_1 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_20_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_20 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_21_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_21 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_22_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_22 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_23_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_23 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_24_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_24 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_25_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_25 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_26_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_26 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_27_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_27 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_28_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_28 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_29_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_29 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_2_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_2 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_30_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_30 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_31_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_31 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_32_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_32 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_33_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_33 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_34_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_34 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_35_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_35 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_36_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_36 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_37_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_37 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_38_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_38 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_39_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_39 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_3_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_3 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_40_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_40 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_41_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_41 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_42_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_42 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_43_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_43 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_44_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_44 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_45_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_45 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_46_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_46 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_47_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_47 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_48_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_48 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_49_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_49 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_4_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_4 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_50_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_50 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_51_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_51 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_52_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_52 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_53_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_53 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_54_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_54 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_55_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_55 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_56_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_56 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_57_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_57 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_58_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_58 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_59_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_59 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_5_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_5 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_60_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_60 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_61_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_61 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_62_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_62 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_63_top_level_namespace_id_repository__idx ON partitions.manifest_references_p_63 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_6_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_6 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_7_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_7 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_8_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_8 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX manifest_references_p_9_top_level_namespace_id_repository_i_idx ON partitions.manifest_references_p_9 USING btree (top_level_namespace_id, repository_id, child_id);

CREATE INDEX index_manifests_on_configuration_blob_digest ON ONLY public.manifests USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_0_configuration_blob_digest_idx ON partitions.manifests_p_0 USING btree (configuration_blob_digest);

CREATE INDEX index_manifests_on_configuration_media_type_id ON ONLY public.manifests USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_0_configuration_media_type_id_idx ON partitions.manifests_p_0 USING btree (configuration_media_type_id);

CREATE INDEX index_manifests_on_media_type_id ON ONLY public.manifests USING btree (media_type_id);

CREATE INDEX manifests_p_0_media_type_id_idx ON partitions.manifests_p_0 USING btree (media_type_id);

CREATE INDEX manifests_p_10_configuration_blob_digest_idx ON partitions.manifests_p_10 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_10_configuration_media_type_id_idx ON partitions.manifests_p_10 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_10_media_type_id_idx ON partitions.manifests_p_10 USING btree (media_type_id);

CREATE INDEX manifests_p_11_configuration_blob_digest_idx ON partitions.manifests_p_11 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_11_configuration_media_type_id_idx ON partitions.manifests_p_11 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_11_media_type_id_idx ON partitions.manifests_p_11 USING btree (media_type_id);

CREATE INDEX manifests_p_12_configuration_blob_digest_idx ON partitions.manifests_p_12 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_12_configuration_media_type_id_idx ON partitions.manifests_p_12 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_12_media_type_id_idx ON partitions.manifests_p_12 USING btree (media_type_id);

CREATE INDEX manifests_p_13_configuration_blob_digest_idx ON partitions.manifests_p_13 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_13_configuration_media_type_id_idx ON partitions.manifests_p_13 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_13_media_type_id_idx ON partitions.manifests_p_13 USING btree (media_type_id);

CREATE INDEX manifests_p_14_configuration_blob_digest_idx ON partitions.manifests_p_14 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_14_configuration_media_type_id_idx ON partitions.manifests_p_14 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_14_media_type_id_idx ON partitions.manifests_p_14 USING btree (media_type_id);

CREATE INDEX manifests_p_15_configuration_blob_digest_idx ON partitions.manifests_p_15 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_15_configuration_media_type_id_idx ON partitions.manifests_p_15 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_15_media_type_id_idx ON partitions.manifests_p_15 USING btree (media_type_id);

CREATE INDEX manifests_p_16_configuration_blob_digest_idx ON partitions.manifests_p_16 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_16_configuration_media_type_id_idx ON partitions.manifests_p_16 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_16_media_type_id_idx ON partitions.manifests_p_16 USING btree (media_type_id);

CREATE INDEX manifests_p_17_configuration_blob_digest_idx ON partitions.manifests_p_17 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_17_configuration_media_type_id_idx ON partitions.manifests_p_17 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_17_media_type_id_idx ON partitions.manifests_p_17 USING btree (media_type_id);

CREATE INDEX manifests_p_18_configuration_blob_digest_idx ON partitions.manifests_p_18 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_18_configuration_media_type_id_idx ON partitions.manifests_p_18 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_18_media_type_id_idx ON partitions.manifests_p_18 USING btree (media_type_id);

CREATE INDEX manifests_p_19_configuration_blob_digest_idx ON partitions.manifests_p_19 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_19_configuration_media_type_id_idx ON partitions.manifests_p_19 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_19_media_type_id_idx ON partitions.manifests_p_19 USING btree (media_type_id);

CREATE INDEX manifests_p_1_configuration_blob_digest_idx ON partitions.manifests_p_1 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_1_configuration_media_type_id_idx ON partitions.manifests_p_1 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_1_media_type_id_idx ON partitions.manifests_p_1 USING btree (media_type_id);

CREATE INDEX manifests_p_20_configuration_blob_digest_idx ON partitions.manifests_p_20 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_20_configuration_media_type_id_idx ON partitions.manifests_p_20 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_20_media_type_id_idx ON partitions.manifests_p_20 USING btree (media_type_id);

CREATE INDEX manifests_p_21_configuration_blob_digest_idx ON partitions.manifests_p_21 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_21_configuration_media_type_id_idx ON partitions.manifests_p_21 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_21_media_type_id_idx ON partitions.manifests_p_21 USING btree (media_type_id);

CREATE INDEX manifests_p_22_configuration_blob_digest_idx ON partitions.manifests_p_22 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_22_configuration_media_type_id_idx ON partitions.manifests_p_22 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_22_media_type_id_idx ON partitions.manifests_p_22 USING btree (media_type_id);

CREATE INDEX manifests_p_23_configuration_blob_digest_idx ON partitions.manifests_p_23 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_23_configuration_media_type_id_idx ON partitions.manifests_p_23 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_23_media_type_id_idx ON partitions.manifests_p_23 USING btree (media_type_id);

CREATE INDEX manifests_p_24_configuration_blob_digest_idx ON partitions.manifests_p_24 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_24_configuration_media_type_id_idx ON partitions.manifests_p_24 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_24_media_type_id_idx ON partitions.manifests_p_24 USING btree (media_type_id);

CREATE INDEX manifests_p_25_configuration_blob_digest_idx ON partitions.manifests_p_25 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_25_configuration_media_type_id_idx ON partitions.manifests_p_25 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_25_media_type_id_idx ON partitions.manifests_p_25 USING btree (media_type_id);

CREATE INDEX manifests_p_26_configuration_blob_digest_idx ON partitions.manifests_p_26 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_26_configuration_media_type_id_idx ON partitions.manifests_p_26 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_26_media_type_id_idx ON partitions.manifests_p_26 USING btree (media_type_id);

CREATE INDEX manifests_p_27_configuration_blob_digest_idx ON partitions.manifests_p_27 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_27_configuration_media_type_id_idx ON partitions.manifests_p_27 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_27_media_type_id_idx ON partitions.manifests_p_27 USING btree (media_type_id);

CREATE INDEX manifests_p_28_configuration_blob_digest_idx ON partitions.manifests_p_28 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_28_configuration_media_type_id_idx ON partitions.manifests_p_28 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_28_media_type_id_idx ON partitions.manifests_p_28 USING btree (media_type_id);

CREATE INDEX manifests_p_29_configuration_blob_digest_idx ON partitions.manifests_p_29 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_29_configuration_media_type_id_idx ON partitions.manifests_p_29 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_29_media_type_id_idx ON partitions.manifests_p_29 USING btree (media_type_id);

CREATE INDEX manifests_p_2_configuration_blob_digest_idx ON partitions.manifests_p_2 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_2_configuration_media_type_id_idx ON partitions.manifests_p_2 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_2_media_type_id_idx ON partitions.manifests_p_2 USING btree (media_type_id);

CREATE INDEX manifests_p_30_configuration_blob_digest_idx ON partitions.manifests_p_30 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_30_configuration_media_type_id_idx ON partitions.manifests_p_30 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_30_media_type_id_idx ON partitions.manifests_p_30 USING btree (media_type_id);

CREATE INDEX manifests_p_31_configuration_blob_digest_idx ON partitions.manifests_p_31 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_31_configuration_media_type_id_idx ON partitions.manifests_p_31 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_31_media_type_id_idx ON partitions.manifests_p_31 USING btree (media_type_id);

CREATE INDEX manifests_p_32_configuration_blob_digest_idx ON partitions.manifests_p_32 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_32_configuration_media_type_id_idx ON partitions.manifests_p_32 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_32_media_type_id_idx ON partitions.manifests_p_32 USING btree (media_type_id);

CREATE INDEX manifests_p_33_configuration_blob_digest_idx ON partitions.manifests_p_33 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_33_configuration_media_type_id_idx ON partitions.manifests_p_33 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_33_media_type_id_idx ON partitions.manifests_p_33 USING btree (media_type_id);

CREATE INDEX manifests_p_34_configuration_blob_digest_idx ON partitions.manifests_p_34 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_34_configuration_media_type_id_idx ON partitions.manifests_p_34 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_34_media_type_id_idx ON partitions.manifests_p_34 USING btree (media_type_id);

CREATE INDEX manifests_p_35_configuration_blob_digest_idx ON partitions.manifests_p_35 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_35_configuration_media_type_id_idx ON partitions.manifests_p_35 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_35_media_type_id_idx ON partitions.manifests_p_35 USING btree (media_type_id);

CREATE INDEX manifests_p_36_configuration_blob_digest_idx ON partitions.manifests_p_36 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_36_configuration_media_type_id_idx ON partitions.manifests_p_36 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_36_media_type_id_idx ON partitions.manifests_p_36 USING btree (media_type_id);

CREATE INDEX manifests_p_37_configuration_blob_digest_idx ON partitions.manifests_p_37 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_37_configuration_media_type_id_idx ON partitions.manifests_p_37 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_37_media_type_id_idx ON partitions.manifests_p_37 USING btree (media_type_id);

CREATE INDEX manifests_p_38_configuration_blob_digest_idx ON partitions.manifests_p_38 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_38_configuration_media_type_id_idx ON partitions.manifests_p_38 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_38_media_type_id_idx ON partitions.manifests_p_38 USING btree (media_type_id);

CREATE INDEX manifests_p_39_configuration_blob_digest_idx ON partitions.manifests_p_39 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_39_configuration_media_type_id_idx ON partitions.manifests_p_39 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_39_media_type_id_idx ON partitions.manifests_p_39 USING btree (media_type_id);

CREATE INDEX manifests_p_3_configuration_blob_digest_idx ON partitions.manifests_p_3 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_3_configuration_media_type_id_idx ON partitions.manifests_p_3 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_3_media_type_id_idx ON partitions.manifests_p_3 USING btree (media_type_id);

CREATE INDEX manifests_p_40_configuration_blob_digest_idx ON partitions.manifests_p_40 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_40_configuration_media_type_id_idx ON partitions.manifests_p_40 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_40_media_type_id_idx ON partitions.manifests_p_40 USING btree (media_type_id);

CREATE INDEX manifests_p_41_configuration_blob_digest_idx ON partitions.manifests_p_41 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_41_configuration_media_type_id_idx ON partitions.manifests_p_41 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_41_media_type_id_idx ON partitions.manifests_p_41 USING btree (media_type_id);

CREATE INDEX manifests_p_42_configuration_blob_digest_idx ON partitions.manifests_p_42 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_42_configuration_media_type_id_idx ON partitions.manifests_p_42 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_42_media_type_id_idx ON partitions.manifests_p_42 USING btree (media_type_id);

CREATE INDEX manifests_p_43_configuration_blob_digest_idx ON partitions.manifests_p_43 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_43_configuration_media_type_id_idx ON partitions.manifests_p_43 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_43_media_type_id_idx ON partitions.manifests_p_43 USING btree (media_type_id);

CREATE INDEX manifests_p_44_configuration_blob_digest_idx ON partitions.manifests_p_44 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_44_configuration_media_type_id_idx ON partitions.manifests_p_44 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_44_media_type_id_idx ON partitions.manifests_p_44 USING btree (media_type_id);

CREATE INDEX manifests_p_45_configuration_blob_digest_idx ON partitions.manifests_p_45 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_45_configuration_media_type_id_idx ON partitions.manifests_p_45 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_45_media_type_id_idx ON partitions.manifests_p_45 USING btree (media_type_id);

CREATE INDEX manifests_p_46_configuration_blob_digest_idx ON partitions.manifests_p_46 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_46_configuration_media_type_id_idx ON partitions.manifests_p_46 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_46_media_type_id_idx ON partitions.manifests_p_46 USING btree (media_type_id);

CREATE INDEX manifests_p_47_configuration_blob_digest_idx ON partitions.manifests_p_47 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_47_configuration_media_type_id_idx ON partitions.manifests_p_47 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_47_media_type_id_idx ON partitions.manifests_p_47 USING btree (media_type_id);

CREATE INDEX manifests_p_48_configuration_blob_digest_idx ON partitions.manifests_p_48 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_48_configuration_media_type_id_idx ON partitions.manifests_p_48 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_48_media_type_id_idx ON partitions.manifests_p_48 USING btree (media_type_id);

CREATE INDEX manifests_p_49_configuration_blob_digest_idx ON partitions.manifests_p_49 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_49_configuration_media_type_id_idx ON partitions.manifests_p_49 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_49_media_type_id_idx ON partitions.manifests_p_49 USING btree (media_type_id);

CREATE INDEX manifests_p_4_configuration_blob_digest_idx ON partitions.manifests_p_4 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_4_configuration_media_type_id_idx ON partitions.manifests_p_4 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_4_media_type_id_idx ON partitions.manifests_p_4 USING btree (media_type_id);

CREATE INDEX manifests_p_50_configuration_blob_digest_idx ON partitions.manifests_p_50 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_50_configuration_media_type_id_idx ON partitions.manifests_p_50 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_50_media_type_id_idx ON partitions.manifests_p_50 USING btree (media_type_id);

CREATE INDEX manifests_p_51_configuration_blob_digest_idx ON partitions.manifests_p_51 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_51_configuration_media_type_id_idx ON partitions.manifests_p_51 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_51_media_type_id_idx ON partitions.manifests_p_51 USING btree (media_type_id);

CREATE INDEX manifests_p_52_configuration_blob_digest_idx ON partitions.manifests_p_52 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_52_configuration_media_type_id_idx ON partitions.manifests_p_52 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_52_media_type_id_idx ON partitions.manifests_p_52 USING btree (media_type_id);

CREATE INDEX manifests_p_53_configuration_blob_digest_idx ON partitions.manifests_p_53 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_53_configuration_media_type_id_idx ON partitions.manifests_p_53 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_53_media_type_id_idx ON partitions.manifests_p_53 USING btree (media_type_id);

CREATE INDEX manifests_p_54_configuration_blob_digest_idx ON partitions.manifests_p_54 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_54_configuration_media_type_id_idx ON partitions.manifests_p_54 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_54_media_type_id_idx ON partitions.manifests_p_54 USING btree (media_type_id);

CREATE INDEX manifests_p_55_configuration_blob_digest_idx ON partitions.manifests_p_55 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_55_configuration_media_type_id_idx ON partitions.manifests_p_55 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_55_media_type_id_idx ON partitions.manifests_p_55 USING btree (media_type_id);

CREATE INDEX manifests_p_56_configuration_blob_digest_idx ON partitions.manifests_p_56 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_56_configuration_media_type_id_idx ON partitions.manifests_p_56 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_56_media_type_id_idx ON partitions.manifests_p_56 USING btree (media_type_id);

CREATE INDEX manifests_p_57_configuration_blob_digest_idx ON partitions.manifests_p_57 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_57_configuration_media_type_id_idx ON partitions.manifests_p_57 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_57_media_type_id_idx ON partitions.manifests_p_57 USING btree (media_type_id);

CREATE INDEX manifests_p_58_configuration_blob_digest_idx ON partitions.manifests_p_58 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_58_configuration_media_type_id_idx ON partitions.manifests_p_58 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_58_media_type_id_idx ON partitions.manifests_p_58 USING btree (media_type_id);

CREATE INDEX manifests_p_59_configuration_blob_digest_idx ON partitions.manifests_p_59 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_59_configuration_media_type_id_idx ON partitions.manifests_p_59 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_59_media_type_id_idx ON partitions.manifests_p_59 USING btree (media_type_id);

CREATE INDEX manifests_p_5_configuration_blob_digest_idx ON partitions.manifests_p_5 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_5_configuration_media_type_id_idx ON partitions.manifests_p_5 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_5_media_type_id_idx ON partitions.manifests_p_5 USING btree (media_type_id);

CREATE INDEX manifests_p_60_configuration_blob_digest_idx ON partitions.manifests_p_60 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_60_configuration_media_type_id_idx ON partitions.manifests_p_60 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_60_media_type_id_idx ON partitions.manifests_p_60 USING btree (media_type_id);

CREATE INDEX manifests_p_61_configuration_blob_digest_idx ON partitions.manifests_p_61 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_61_configuration_media_type_id_idx ON partitions.manifests_p_61 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_61_media_type_id_idx ON partitions.manifests_p_61 USING btree (media_type_id);

CREATE INDEX manifests_p_62_configuration_blob_digest_idx ON partitions.manifests_p_62 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_62_configuration_media_type_id_idx ON partitions.manifests_p_62 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_62_media_type_id_idx ON partitions.manifests_p_62 USING btree (media_type_id);

CREATE INDEX manifests_p_63_configuration_blob_digest_idx ON partitions.manifests_p_63 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_63_configuration_media_type_id_idx ON partitions.manifests_p_63 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_63_media_type_id_idx ON partitions.manifests_p_63 USING btree (media_type_id);

CREATE INDEX manifests_p_6_configuration_blob_digest_idx ON partitions.manifests_p_6 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_6_configuration_media_type_id_idx ON partitions.manifests_p_6 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_6_media_type_id_idx ON partitions.manifests_p_6 USING btree (media_type_id);

CREATE INDEX manifests_p_7_configuration_blob_digest_idx ON partitions.manifests_p_7 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_7_configuration_media_type_id_idx ON partitions.manifests_p_7 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_7_media_type_id_idx ON partitions.manifests_p_7 USING btree (media_type_id);

CREATE INDEX manifests_p_8_configuration_blob_digest_idx ON partitions.manifests_p_8 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_8_configuration_media_type_id_idx ON partitions.manifests_p_8 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_8_media_type_id_idx ON partitions.manifests_p_8 USING btree (media_type_id);

CREATE INDEX manifests_p_9_configuration_blob_digest_idx ON partitions.manifests_p_9 USING btree (configuration_blob_digest);

CREATE INDEX manifests_p_9_configuration_media_type_id_idx ON partitions.manifests_p_9 USING btree (configuration_media_type_id);

CREATE INDEX manifests_p_9_media_type_id_idx ON partitions.manifests_p_9 USING btree (media_type_id);

CREATE INDEX index_repository_blobs_on_blob_digest ON ONLY public.repository_blobs USING btree (blob_digest);

CREATE INDEX repository_blobs_p_0_blob_digest_idx ON partitions.repository_blobs_p_0 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_10_blob_digest_idx ON partitions.repository_blobs_p_10 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_11_blob_digest_idx ON partitions.repository_blobs_p_11 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_12_blob_digest_idx ON partitions.repository_blobs_p_12 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_13_blob_digest_idx ON partitions.repository_blobs_p_13 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_14_blob_digest_idx ON partitions.repository_blobs_p_14 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_15_blob_digest_idx ON partitions.repository_blobs_p_15 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_16_blob_digest_idx ON partitions.repository_blobs_p_16 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_17_blob_digest_idx ON partitions.repository_blobs_p_17 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_18_blob_digest_idx ON partitions.repository_blobs_p_18 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_19_blob_digest_idx ON partitions.repository_blobs_p_19 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_1_blob_digest_idx ON partitions.repository_blobs_p_1 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_20_blob_digest_idx ON partitions.repository_blobs_p_20 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_21_blob_digest_idx ON partitions.repository_blobs_p_21 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_22_blob_digest_idx ON partitions.repository_blobs_p_22 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_23_blob_digest_idx ON partitions.repository_blobs_p_23 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_24_blob_digest_idx ON partitions.repository_blobs_p_24 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_25_blob_digest_idx ON partitions.repository_blobs_p_25 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_26_blob_digest_idx ON partitions.repository_blobs_p_26 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_27_blob_digest_idx ON partitions.repository_blobs_p_27 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_28_blob_digest_idx ON partitions.repository_blobs_p_28 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_29_blob_digest_idx ON partitions.repository_blobs_p_29 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_2_blob_digest_idx ON partitions.repository_blobs_p_2 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_30_blob_digest_idx ON partitions.repository_blobs_p_30 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_31_blob_digest_idx ON partitions.repository_blobs_p_31 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_32_blob_digest_idx ON partitions.repository_blobs_p_32 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_33_blob_digest_idx ON partitions.repository_blobs_p_33 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_34_blob_digest_idx ON partitions.repository_blobs_p_34 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_35_blob_digest_idx ON partitions.repository_blobs_p_35 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_36_blob_digest_idx ON partitions.repository_blobs_p_36 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_37_blob_digest_idx ON partitions.repository_blobs_p_37 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_38_blob_digest_idx ON partitions.repository_blobs_p_38 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_39_blob_digest_idx ON partitions.repository_blobs_p_39 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_3_blob_digest_idx ON partitions.repository_blobs_p_3 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_40_blob_digest_idx ON partitions.repository_blobs_p_40 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_41_blob_digest_idx ON partitions.repository_blobs_p_41 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_42_blob_digest_idx ON partitions.repository_blobs_p_42 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_43_blob_digest_idx ON partitions.repository_blobs_p_43 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_44_blob_digest_idx ON partitions.repository_blobs_p_44 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_45_blob_digest_idx ON partitions.repository_blobs_p_45 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_46_blob_digest_idx ON partitions.repository_blobs_p_46 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_47_blob_digest_idx ON partitions.repository_blobs_p_47 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_48_blob_digest_idx ON partitions.repository_blobs_p_48 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_49_blob_digest_idx ON partitions.repository_blobs_p_49 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_4_blob_digest_idx ON partitions.repository_blobs_p_4 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_50_blob_digest_idx ON partitions.repository_blobs_p_50 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_51_blob_digest_idx ON partitions.repository_blobs_p_51 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_52_blob_digest_idx ON partitions.repository_blobs_p_52 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_53_blob_digest_idx ON partitions.repository_blobs_p_53 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_54_blob_digest_idx ON partitions.repository_blobs_p_54 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_55_blob_digest_idx ON partitions.repository_blobs_p_55 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_56_blob_digest_idx ON partitions.repository_blobs_p_56 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_57_blob_digest_idx ON partitions.repository_blobs_p_57 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_58_blob_digest_idx ON partitions.repository_blobs_p_58 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_59_blob_digest_idx ON partitions.repository_blobs_p_59 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_5_blob_digest_idx ON partitions.repository_blobs_p_5 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_60_blob_digest_idx ON partitions.repository_blobs_p_60 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_61_blob_digest_idx ON partitions.repository_blobs_p_61 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_62_blob_digest_idx ON partitions.repository_blobs_p_62 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_63_blob_digest_idx ON partitions.repository_blobs_p_63 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_6_blob_digest_idx ON partitions.repository_blobs_p_6 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_7_blob_digest_idx ON partitions.repository_blobs_p_7 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_8_blob_digest_idx ON partitions.repository_blobs_p_8 USING btree (blob_digest);

CREATE INDEX repository_blobs_p_9_blob_digest_idx ON partitions.repository_blobs_p_9 USING btree (blob_digest);

CREATE INDEX index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ON ONLY public.tags USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_0_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_0 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_10_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_10 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_11_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_11 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_12_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_12 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_13_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_13 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_14_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_14 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_15_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_15 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_16_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_16 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_17_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_17 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_18_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_18 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_19_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_19 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_1_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_1 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_20_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_20 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_21_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_21 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_22_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_22 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_23_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_23 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_24_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_24 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_25_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_25 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_26_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_26 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_27_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_27 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_28_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_28 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_29_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_29 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_2_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_2 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_30_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_30 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_31_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_31 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_32_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_32 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_33_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_33 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_34_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_34 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_35_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_35 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_36_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_36 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_37_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_37 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_38_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_38 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_39_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_39 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_3_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_3 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_40_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_40 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_41_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_41 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_42_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_42 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_43_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_43 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_44_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_44 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_45_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_45 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_46_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_46 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_47_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_47 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_48_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_48 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_49_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_49 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_4_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_4 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_50_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_50 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_51_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_51 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_52_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_52 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_53_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_53 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_54_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_54 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_55_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_55 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_56_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_56 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_57_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_57 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_58_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_58 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_59_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_59 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_5_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_5 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_60_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_60 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_61_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_61 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_62_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_62 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_63_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_63 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_6_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_6 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_7_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_7 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_8_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_8 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX tags_p_9_top_level_namespace_id_repository_id_manifest_id_idx ON partitions.tags_p_9 USING btree (top_level_namespace_id, repository_id, manifest_id);

CREATE INDEX index_gc_blob_review_queue_on_review_after ON public.gc_blob_review_queue USING btree (review_after);

CREATE INDEX index_gc_manifest_review_queue_on_review_after ON public.gc_manifest_review_queue USING btree (review_after);

CREATE INDEX index_repositories_on_id_where_deleted_at_not_null ON public.repositories USING btree (id)
WHERE (deleted_at IS NOT NULL);

CREATE INDEX index_repositories_on_top_level_namespace_id_and_parent_id ON public.repositories USING btree (top_level_namespace_id, parent_id);

CREATE INDEX index_repositories_on_top_level_namespace_id_and_path_and_id ON public.repositories USING btree (top_level_namespace_id, path text_pattern_ops, id);

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_0_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_0_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_10_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_10_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_11_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_11_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_12_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_12_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_13_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_13_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_14_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_14_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_15_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_15_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_16_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_16_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_17_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_17_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_18_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_18_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_19_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_19_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_1_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_1_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_20_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_20_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_21_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_21_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_22_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_22_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_23_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_23_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_24_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_24_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_25_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_25_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_26_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_26_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_27_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_27_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_28_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_28_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_29_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_29_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_2_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_2_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_30_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_30_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_31_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_31_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_32_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_32_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_33_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_33_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_34_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_34_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_35_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_35_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_36_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_36_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_37_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_37_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_38_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_38_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_39_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_39_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_3_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_3_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_40_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_40_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_41_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_41_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_42_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_42_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_43_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_43_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_44_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_44_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_45_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_45_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_46_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_46_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_47_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_47_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_48_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_48_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_49_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_49_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_4_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_4_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_50_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_50_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_51_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_51_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_52_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_52_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_53_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_53_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_54_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_54_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_55_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_55_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_56_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_56_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_57_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_57_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_58_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_58_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_59_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_59_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_5_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_5_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_60_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_60_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_61_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_61_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_62_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_62_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_63_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_63_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_6_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_6_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_7_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_7_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_8_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_8_pkey;

ALTER INDEX public.index_blobs_on_media_type_id ATTACH PARTITION partitions.blobs_p_9_media_type_id_idx;

ALTER INDEX public.pk_blobs ATTACH PARTITION partitions.blobs_p_9_pkey;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_0_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_0_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_0_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_10_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_10_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_10_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_11_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_11_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_11_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_12_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_12_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_12_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_13_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_13_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_13_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_14_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_14_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_14_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_15_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_15_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_15_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_16_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_16_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_16_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_17_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_17_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_17_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_18_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_18_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_18_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_19_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_19_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_19_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_1_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_1_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_1_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_20_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_20_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_20_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_21_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_21_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_21_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_22_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_22_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_22_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_23_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_23_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_23_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_24_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_24_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_24_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_25_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_25_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_25_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_26_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_26_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_26_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_27_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_27_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_27_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_28_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_28_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_28_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_29_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_29_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_29_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_2_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_2_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_2_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_30_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_30_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_30_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_31_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_31_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_31_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_32_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_32_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_32_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_33_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_33_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_33_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_34_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_34_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_34_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_35_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_35_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_35_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_36_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_36_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_36_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_37_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_37_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_37_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_38_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_38_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_38_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_39_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_39_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_39_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_3_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_3_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_3_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_40_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_40_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_40_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_41_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_41_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_41_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_42_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_42_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_42_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_43_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_43_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_43_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_44_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_44_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_44_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_45_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_45_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_45_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_46_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_46_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_46_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_47_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_47_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_47_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_48_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_48_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_48_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_49_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_49_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_49_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_4_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_4_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_4_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_50_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_50_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_50_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_51_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_51_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_51_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_52_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_52_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_52_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_53_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_53_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_53_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_54_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_54_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_54_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_55_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_55_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_55_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_56_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_56_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_56_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_57_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_57_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_57_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_58_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_58_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_58_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_59_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_59_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_59_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_5_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_5_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_5_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_60_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_60_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_60_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_61_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_61_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_61_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_62_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_62_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_62_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_63_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_63_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_63_top_level_namespace_id_reposit_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_6_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_6_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_6_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_7_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_7_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_7_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_8_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_8_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_8_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_configurations_digest_and_manifest_id ATTACH PARTITION partitions.gc_blobs_configurations_p_9_digest_manifest_id_key;

ALTER INDEX public.pk_gc_blobs_configurations ATTACH PARTITION partitions.gc_blobs_configurations_p_9_pkey;

ALTER INDEX public.index_gc_blobs_configurations_on_tp_lvl_nmspc_id_r_id_m_id_dgst ATTACH PARTITION partitions.gc_blobs_configurations_p_9_top_level_namespace_id_reposito_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_0_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_0_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_0_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_10_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_10_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_10_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_11_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_11_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_11_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_12_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_12_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_12_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_13_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_13_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_13_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_14_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_14_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_14_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_15_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_15_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_15_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_16_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_16_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_16_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_17_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_17_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_17_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_18_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_18_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_18_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_19_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_19_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_19_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_1_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_1_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_1_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_20_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_20_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_20_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_21_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_21_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_21_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_22_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_22_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_22_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_23_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_23_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_23_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_24_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_24_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_24_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_25_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_25_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_25_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_26_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_26_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_26_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_27_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_27_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_27_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_28_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_28_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_28_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_29_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_29_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_29_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_2_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_2_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_2_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_30_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_30_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_30_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_31_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_31_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_31_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_32_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_32_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_32_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_33_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_33_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_33_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_34_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_34_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_34_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_35_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_35_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_35_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_36_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_36_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_36_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_37_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_37_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_37_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_38_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_38_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_38_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_39_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_39_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_39_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_3_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_3_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_3_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_40_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_40_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_40_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_41_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_41_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_41_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_42_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_42_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_42_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_43_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_43_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_43_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_44_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_44_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_44_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_45_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_45_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_45_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_46_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_46_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_46_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_47_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_47_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_47_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_48_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_48_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_48_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_49_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_49_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_49_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_4_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_4_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_4_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_50_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_50_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_50_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_51_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_51_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_51_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_52_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_52_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_52_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_53_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_53_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_53_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_54_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_54_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_54_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_55_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_55_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_55_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_56_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_56_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_56_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_57_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_57_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_57_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_58_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_58_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_58_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_59_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_59_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_59_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_5_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_5_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_5_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_60_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_60_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_60_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_61_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_61_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_61_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_62_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_62_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_62_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_63_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_63_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_63_top_level_namespace_id_repository_id_l_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_6_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_6_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_6_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_7_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_7_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_7_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_8_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_8_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_8_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.unique_gc_blobs_layers_digest_and_layer_id ATTACH PARTITION partitions.gc_blobs_layers_p_9_digest_layer_id_key;

ALTER INDEX public.pk_gc_blobs_layers ATTACH PARTITION partitions.gc_blobs_layers_p_9_pkey;

ALTER INDEX public.index_gc_blobs_layers_on_top_lvl_nmspc_id_rpstry_id_lyr_id_dgst ATTACH PARTITION partitions.gc_blobs_layers_p_9_top_level_namespace_id_repository_id_la_idx;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_0_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_10_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_11_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_12_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_13_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_14_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_15_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_16_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_17_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_18_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_19_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_1_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_20_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_21_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_22_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_23_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_24_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_25_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_26_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_27_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_28_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_29_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_2_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_30_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_31_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_32_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_33_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_34_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_35_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_36_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_37_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_38_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_39_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_3_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_40_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_41_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_42_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_43_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_44_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_45_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_46_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_47_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_48_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_49_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_4_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_50_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_51_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_52_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_53_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_54_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_55_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_56_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_57_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_58_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_59_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_5_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_60_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_61_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_62_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_63_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_6_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_7_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_8_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_layers_on_top_level_namespace_id_and_digest_and_size ATTACH PARTITION partitions.index_layers_p_9_on_top_level_namespace_id_and_digest_and_size;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_10_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_11_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_12_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_13_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_14_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_15_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_16_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_17_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_18_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_19_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_20_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_21_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_22_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_23_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_24_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_25_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_26_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_27_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_28_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_29_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_30_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_31_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_32_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_33_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_34_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_35_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_36_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_37_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_38_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_39_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_40_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_41_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_42_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_43_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_44_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_45_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_46_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_47_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_48_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_49_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_4_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_50_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_51_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_52_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_53_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_54_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_55_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_56_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_57_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_58_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_59_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_5_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_60_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_61_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_62_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_63_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_6_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_7_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_8_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ATTACH PARTITION partitions.index_tags_p_9_on_ns_id_and_repo_id_and_manifest_id_and_name;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_0_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_0_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_0_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_0_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_0_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_10_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_10_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_10_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_10_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_10_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_11_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_11_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_11_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_11_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_11_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_12_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_12_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_12_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_12_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_12_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_13_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_13_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_13_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_13_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_13_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_14_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_14_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_14_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_14_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_14_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_15_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_15_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_15_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_15_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_15_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_16_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_16_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_16_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_16_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_16_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_17_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_17_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_17_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_17_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_17_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_18_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_18_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_18_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_18_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_18_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_19_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_19_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_19_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_19_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_19_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_1_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_1_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_1_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_1_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_1_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_20_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_20_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_20_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_20_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_20_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_21_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_21_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_21_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_21_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_21_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_22_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_22_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_22_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_22_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_22_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_23_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_23_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_23_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_23_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_23_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_24_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_24_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_24_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_24_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_24_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_25_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_25_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_25_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_25_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_25_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_26_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_26_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_26_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_26_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_26_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_27_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_27_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_27_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_27_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_27_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_28_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_28_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_28_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_28_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_28_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_29_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_29_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_29_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_29_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_29_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_2_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_2_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_2_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_2_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_2_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_30_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_30_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_30_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_30_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_30_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_31_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_31_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_31_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_31_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_31_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_32_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_32_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_32_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_32_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_32_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_33_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_33_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_33_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_33_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_33_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_34_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_34_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_34_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_34_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_34_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_35_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_35_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_35_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_35_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_35_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_36_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_36_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_36_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_36_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_36_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_37_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_37_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_37_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_37_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_37_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_38_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_38_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_38_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_38_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_38_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_39_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_39_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_39_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_39_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_39_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_3_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_3_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_3_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_3_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_3_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_40_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_40_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_40_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_40_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_40_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_41_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_41_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_41_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_41_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_41_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_42_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_42_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_42_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_42_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_42_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_43_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_43_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_43_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_43_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_43_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_44_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_44_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_44_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_44_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_44_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_45_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_45_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_45_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_45_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_45_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_46_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_46_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_46_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_46_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_46_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_47_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_47_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_47_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_47_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_47_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_48_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_48_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_48_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_48_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_48_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_49_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_49_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_49_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_49_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_49_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_4_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_4_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_4_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_4_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_4_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_50_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_50_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_50_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_50_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_50_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_51_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_51_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_51_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_51_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_51_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_52_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_52_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_52_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_52_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_52_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_53_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_53_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_53_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_53_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_53_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_54_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_54_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_54_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_54_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_54_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_55_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_55_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_55_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_55_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_55_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_56_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_56_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_56_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_56_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_56_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_57_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_57_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_57_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_57_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_57_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_58_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_58_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_58_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_58_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_58_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_59_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_59_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_59_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_59_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_59_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_5_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_5_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_5_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_5_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_5_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_60_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_60_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_60_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_60_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_60_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_61_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_61_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_61_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_61_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_61_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_62_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_62_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_62_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_62_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_62_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_63_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_63_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_63_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_63_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_63_top_level_namespace_id_repository_id_manifest_i_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_6_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_6_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_6_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_6_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_6_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_7_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_7_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_7_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_7_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_7_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_8_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_8_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_8_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_8_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_8_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.index_layers_on_digest ATTACH PARTITION partitions.layers_p_9_digest_idx;

ALTER INDEX public.index_layers_on_media_type_id ATTACH PARTITION partitions.layers_p_9_media_type_id_idx;

ALTER INDEX public.pk_layers ATTACH PARTITION partitions.layers_p_9_pkey;

ALTER INDEX public.unique_layers_top_lvl_nmspc_id_and_rpstory_id_and_id_and_digest ATTACH PARTITION partitions.layers_p_9_top_level_namespace_id_repository_id_id_digest_key;

ALTER INDEX public.unique_layers_tp_lvl_nmspc_id_rpstry_id_and_mnfst_id_and_digest ATTACH PARTITION partitions.layers_p_9_top_level_namespace_id_repository_id_manifest_id_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_0_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_0_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_0_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_10_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_10_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_10_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_11_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_11_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_11_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_12_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_12_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_12_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_13_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_13_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_13_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_14_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_14_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_14_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_15_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_15_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_15_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_16_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_16_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_16_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_17_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_17_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_17_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_18_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_18_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_18_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_19_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_19_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_19_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_1_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_1_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_1_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_20_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_20_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_20_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_21_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_21_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_21_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_22_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_22_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_22_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_23_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_23_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_23_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_24_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_24_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_24_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_25_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_25_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_25_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_26_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_26_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_26_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_27_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_27_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_27_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_28_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_28_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_28_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_29_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_29_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_29_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_2_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_2_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_2_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_30_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_30_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_30_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_31_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_31_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_31_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_32_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_32_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_32_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_33_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_33_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_33_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_34_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_34_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_34_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_35_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_35_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_35_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_36_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_36_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_36_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_37_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_37_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_37_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_38_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_38_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_38_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_39_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_39_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_39_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_3_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_3_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_3_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_40_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_40_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_40_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_41_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_41_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_41_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_42_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_42_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_42_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_43_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_43_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_43_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_44_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_44_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_44_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_45_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_45_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_45_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_46_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_46_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_46_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_47_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_47_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_47_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_48_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_48_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_48_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_49_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_49_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_49_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_4_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_4_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_4_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_50_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_50_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_50_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_51_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_51_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_51_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_52_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_52_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_52_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_53_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_53_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_53_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_54_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_54_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_54_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_55_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_55_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_55_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_56_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_56_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_56_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_57_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_57_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_57_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_58_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_58_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_58_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_59_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_59_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_59_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_5_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_5_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_5_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_60_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_60_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_60_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_61_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_61_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_61_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_62_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_62_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_62_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_63_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_63_top_level_namespace_id_repository__idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_63_top_level_namespace_id_repository__key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_6_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_6_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_6_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_7_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_7_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_7_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_8_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_8_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_8_top_level_namespace_id_repository_i_key;

ALTER INDEX public.pk_manifest_references ATTACH PARTITION partitions.manifest_references_p_9_pkey;

ALTER INDEX public.index_manifest_references_on_tp_lvl_nmspc_id_rpstry_id_chld_id ATTACH PARTITION partitions.manifest_references_p_9_top_level_namespace_id_repository_i_idx;

ALTER INDEX public.unique_manifest_references_tp_lvl_nmspc_id_rpy_id_prt_id_chd_id ATTACH PARTITION partitions.manifest_references_p_9_top_level_namespace_id_repository_i_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_0_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_0_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_0_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_0_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_0_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_0_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_10_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_10_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_10_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_10_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_10_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_10_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_11_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_11_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_11_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_11_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_11_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_11_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_12_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_12_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_12_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_12_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_12_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_12_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_13_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_13_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_13_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_13_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_13_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_13_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_14_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_14_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_14_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_14_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_14_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_14_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_15_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_15_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_15_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_15_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_15_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_15_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_16_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_16_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_16_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_16_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_16_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_16_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_17_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_17_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_17_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_17_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_17_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_17_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_18_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_18_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_18_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_18_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_18_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_18_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_19_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_19_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_19_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_19_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_19_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_19_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_1_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_1_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_1_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_1_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_1_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_1_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_20_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_20_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_20_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_20_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_20_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_20_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_21_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_21_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_21_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_21_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_21_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_21_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_22_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_22_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_22_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_22_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_22_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_22_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_23_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_23_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_23_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_23_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_23_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_23_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_24_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_24_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_24_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_24_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_24_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_24_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_25_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_25_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_25_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_25_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_25_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_25_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_26_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_26_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_26_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_26_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_26_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_26_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_27_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_27_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_27_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_27_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_27_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_27_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_28_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_28_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_28_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_28_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_28_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_28_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_29_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_29_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_29_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_29_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_29_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_29_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_2_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_2_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_2_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_2_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_2_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_2_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_30_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_30_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_30_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_30_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_30_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_30_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_31_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_31_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_31_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_31_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_31_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_31_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_32_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_32_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_32_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_32_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_32_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_32_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_33_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_33_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_33_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_33_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_33_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_33_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_34_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_34_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_34_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_34_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_34_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_34_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_35_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_35_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_35_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_35_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_35_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_35_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_36_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_36_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_36_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_36_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_36_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_36_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_37_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_37_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_37_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_37_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_37_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_37_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_38_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_38_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_38_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_38_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_38_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_38_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_39_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_39_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_39_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_39_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_39_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_39_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_3_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_3_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_3_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_3_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_3_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_3_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_40_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_40_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_40_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_40_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_40_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_40_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_41_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_41_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_41_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_41_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_41_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_41_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_42_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_42_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_42_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_42_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_42_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_42_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_43_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_43_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_43_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_43_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_43_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_43_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_44_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_44_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_44_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_44_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_44_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_44_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_45_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_45_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_45_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_45_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_45_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_45_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_46_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_46_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_46_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_46_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_46_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_46_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_47_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_47_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_47_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_47_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_47_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_47_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_48_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_48_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_48_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_48_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_48_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_48_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_49_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_49_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_49_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_49_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_49_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_49_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_4_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_4_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_4_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_4_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_4_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_4_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_50_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_50_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_50_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_50_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_50_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_50_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_51_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_51_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_51_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_51_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_51_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_51_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_52_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_52_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_52_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_52_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_52_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_52_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_53_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_53_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_53_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_53_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_53_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_53_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_54_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_54_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_54_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_54_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_54_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_54_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_55_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_55_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_55_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_55_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_55_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_55_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_56_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_56_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_56_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_56_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_56_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_56_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_57_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_57_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_57_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_57_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_57_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_57_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_58_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_58_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_58_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_58_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_58_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_58_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_59_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_59_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_59_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_59_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_59_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_59_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_5_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_5_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_5_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_5_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_5_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_5_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_60_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_60_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_60_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_60_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_60_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_60_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_61_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_61_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_61_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_61_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_61_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_61_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_62_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_62_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_62_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_62_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_62_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_62_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_63_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_63_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_63_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_63_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_63_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_63_top_level_namespace_id_repository_id_id_conf_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_6_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_6_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_6_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_6_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_6_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_6_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_7_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_7_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_7_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_7_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_7_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_7_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_8_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_8_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_8_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_8_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_8_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_8_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_manifests_on_configuration_blob_digest ATTACH PARTITION partitions.manifests_p_9_configuration_blob_digest_idx;

ALTER INDEX public.index_manifests_on_configuration_media_type_id ATTACH PARTITION partitions.manifests_p_9_configuration_media_type_id_idx;

ALTER INDEX public.index_manifests_on_media_type_id ATTACH PARTITION partitions.manifests_p_9_media_type_id_idx;

ALTER INDEX public.pk_manifests ATTACH PARTITION partitions.manifests_p_9_pkey;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repository_id_and_digest ATTACH PARTITION partitions.manifests_p_9_top_level_namespace_id_repository_id_digest_key;

ALTER INDEX public.unique_manifests_top_lvl_nmspc_id_and_repo_id_id_cfg_blob_dgst ATTACH PARTITION partitions.manifests_p_9_top_level_namespace_id_repository_id_id_confi_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_0_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_0_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_0_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_10_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_10_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_10_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_11_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_11_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_11_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_12_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_12_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_12_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_13_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_13_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_13_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_14_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_14_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_14_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_15_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_15_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_15_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_16_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_16_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_16_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_17_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_17_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_17_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_18_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_18_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_18_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_19_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_19_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_19_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_1_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_1_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_1_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_20_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_20_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_20_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_21_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_21_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_21_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_22_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_22_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_22_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_23_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_23_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_23_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_24_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_24_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_24_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_25_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_25_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_25_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_26_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_26_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_26_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_27_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_27_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_27_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_28_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_28_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_28_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_29_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_29_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_29_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_2_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_2_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_2_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_30_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_30_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_30_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_31_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_31_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_31_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_32_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_32_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_32_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_33_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_33_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_33_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_34_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_34_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_34_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_35_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_35_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_35_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_36_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_36_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_36_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_37_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_37_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_37_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_38_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_38_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_38_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_39_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_39_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_39_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_3_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_3_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_3_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_40_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_40_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_40_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_41_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_41_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_41_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_42_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_42_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_42_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_43_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_43_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_43_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_44_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_44_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_44_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_45_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_45_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_45_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_46_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_46_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_46_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_47_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_47_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_47_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_48_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_48_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_48_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_49_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_49_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_49_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_4_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_4_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_4_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_50_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_50_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_50_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_51_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_51_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_51_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_52_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_52_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_52_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_53_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_53_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_53_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_54_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_54_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_54_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_55_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_55_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_55_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_56_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_56_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_56_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_57_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_57_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_57_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_58_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_58_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_58_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_59_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_59_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_59_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_5_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_5_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_5_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_60_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_60_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_60_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_61_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_61_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_61_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_62_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_62_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_62_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_63_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_63_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_63_top_level_namespace_id_repository_id__key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_6_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_6_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_6_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_7_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_7_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_7_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_8_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_8_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_8_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.index_repository_blobs_on_blob_digest ATTACH PARTITION partitions.repository_blobs_p_9_blob_digest_idx;

ALTER INDEX public.pk_repository_blobs ATTACH PARTITION partitions.repository_blobs_p_9_pkey;

ALTER INDEX public.unique_repository_blobs_tp_lvl_nmspc_id_and_rpstry_id_blb_dgst ATTACH PARTITION partitions.repository_blobs_p_9_top_level_namespace_id_repository_id_b_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_0_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_0_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_0_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_10_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_10_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_10_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_11_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_11_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_11_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_12_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_12_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_12_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_13_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_13_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_13_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_14_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_14_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_14_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_15_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_15_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_15_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_16_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_16_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_16_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_17_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_17_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_17_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_18_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_18_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_18_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_19_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_19_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_19_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_1_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_1_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_1_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_20_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_20_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_20_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_21_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_21_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_21_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_22_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_22_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_22_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_23_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_23_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_23_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_24_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_24_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_24_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_25_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_25_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_25_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_26_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_26_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_26_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_27_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_27_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_27_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_28_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_28_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_28_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_29_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_29_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_29_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_2_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_2_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_2_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_30_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_30_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_30_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_31_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_31_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_31_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_32_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_32_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_32_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_33_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_33_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_33_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_34_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_34_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_34_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_35_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_35_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_35_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_36_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_36_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_36_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_37_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_37_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_37_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_38_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_38_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_38_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_39_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_39_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_39_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_3_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_3_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_3_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_40_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_40_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_40_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_41_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_41_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_41_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_42_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_42_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_42_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_43_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_43_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_43_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_44_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_44_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_44_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_45_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_45_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_45_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_46_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_46_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_46_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_47_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_47_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_47_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_48_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_48_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_48_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_49_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_49_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_49_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_4_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_4_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_4_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_50_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_50_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_50_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_51_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_51_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_51_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_52_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_52_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_52_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_53_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_53_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_53_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_54_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_54_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_54_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_55_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_55_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_55_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_56_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_56_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_56_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_57_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_57_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_57_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_58_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_58_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_58_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_59_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_59_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_59_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_5_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_5_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_5_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_60_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_60_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_60_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_61_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_61_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_61_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_62_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_62_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_62_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_63_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_63_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_63_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_6_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_6_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_6_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_7_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_7_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_7_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_8_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_8_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_8_top_level_namespace_id_repository_id_name_key;

ALTER INDEX public.pk_tags ATTACH PARTITION partitions.tags_p_9_pkey;

ALTER INDEX public.index_tags_on_top_lvl_nmspc_id_and_rpository_id_and_manifest_id ATTACH PARTITION partitions.tags_p_9_top_level_namespace_id_repository_id_manifest_id_idx;

ALTER INDEX public.unique_tags_top_level_namespace_id_and_repository_id_and_name ATTACH PARTITION partitions.tags_p_9_top_level_namespace_id_repository_id_name_key;

CREATE TRIGGER gc_track_blob_uploads_trigger
    AFTER INSERT ON public.blobs
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_blob_uploads ();

CREATE TRIGGER gc_track_configuration_blobs_trigger
    AFTER INSERT ON public.manifests
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_configuration_blobs ();

CREATE TRIGGER gc_track_deleted_layers_trigger
    AFTER DELETE ON public.layers REFERENCING OLD TABLE AS old_table
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.gc_track_deleted_layers ();

CREATE TRIGGER gc_track_deleted_manifest_lists_trigger
    AFTER DELETE ON public.manifest_references
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_deleted_manifest_lists ();

CREATE TRIGGER gc_track_deleted_manifests_trigger
    AFTER DELETE ON public.manifests
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_deleted_manifests ();

CREATE TRIGGER gc_track_deleted_tags_trigger
    AFTER DELETE ON public.tags
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_deleted_tags ();

CREATE TRIGGER gc_track_layer_blobs_trigger
    AFTER INSERT ON public.layers
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_layer_blobs ();

CREATE TRIGGER gc_track_manifest_uploads_trigger
    AFTER INSERT ON public.manifests
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_manifest_uploads ();

CREATE TRIGGER gc_track_switched_tags_trigger
    AFTER UPDATE OF manifest_id ON public.tags
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_switched_tags ();

CREATE TRIGGER gc_track_tmp_blobs_manifests_trigger
    AFTER INSERT ON public.manifests
    FOR EACH ROW
    EXECUTE FUNCTION public.gc_track_tmp_blobs_manifests ();

ALTER TABLE public.blobs
    ADD CONSTRAINT fk_blobs_media_type_id_media_types FOREIGN KEY (media_type_id) REFERENCES public.media_types (id);

ALTER TABLE public.gc_blobs_configurations
    ADD CONSTRAINT fk_gc_blobs_configurations_digest_blobs FOREIGN KEY (digest) REFERENCES public.blobs (digest) ON DELETE CASCADE;

ALTER TABLE public.gc_blobs_configurations
    ADD CONSTRAINT fk_gc_blobs_configurations_tp_lvl_nspc_id_r_id_m_id_dgst_mnfsts FOREIGN KEY (top_level_namespace_id, repository_id, manifest_id, digest) REFERENCES public.manifests (top_level_namespace_id, repository_id, id, configuration_blob_digest) ON DELETE CASCADE;

ALTER TABLE public.gc_blobs_layers
    ADD CONSTRAINT fk_gc_blobs_layers_digest_blobs FOREIGN KEY (digest) REFERENCES public.blobs (digest) ON DELETE CASCADE;

ALTER TABLE public.gc_blobs_layers
    ADD CONSTRAINT fk_gc_blobs_layers_top_lvl_nmspc_id_rptry_id_lyr_id_dgst_layers FOREIGN KEY (top_level_namespace_id, repository_id, layer_id, digest) REFERENCES public.layers (top_level_namespace_id, repository_id, id, digest) ON DELETE CASCADE;

ALTER TABLE ONLY public.gc_manifest_review_queue
    ADD CONSTRAINT fk_gc_manifest_review_queue_tp_lvl_nspc_id_rp_id_mfst_id_mnfsts FOREIGN KEY (top_level_namespace_id, repository_id, manifest_id) REFERENCES public.manifests (top_level_namespace_id, repository_id, id) ON DELETE CASCADE;

ALTER TABLE public.layers
    ADD CONSTRAINT fk_layers_digest_blobs FOREIGN KEY (digest) REFERENCES public.blobs (digest);

ALTER TABLE public.layers
    ADD CONSTRAINT fk_layers_media_type_id_media_types FOREIGN KEY (media_type_id) REFERENCES public.media_types (id);

ALTER TABLE public.layers
    ADD CONSTRAINT fk_layers_top_lvl_nmspc_id_and_repo_id_and_manifst_id_manifests FOREIGN KEY (top_level_namespace_id, repository_id, manifest_id) REFERENCES public.manifests (top_level_namespace_id, repository_id, id) ON DELETE CASCADE;

ALTER TABLE public.manifest_references
    ADD CONSTRAINT fk_manifest_references_tp_lvl_nmspc_id_rpsty_id_chld_id_mnfsts FOREIGN KEY (top_level_namespace_id, repository_id, child_id) REFERENCES public.manifests (top_level_namespace_id, repository_id, id);

ALTER TABLE public.manifest_references
    ADD CONSTRAINT fk_manifest_references_tp_lvl_nmspc_id_rpsty_id_prnt_id_mnfsts FOREIGN KEY (top_level_namespace_id, repository_id, parent_id) REFERENCES public.manifests (top_level_namespace_id, repository_id, id) ON DELETE CASCADE;

ALTER TABLE public.manifests
    ADD CONSTRAINT fk_manifests_configuration_blob_digest_blobs FOREIGN KEY (configuration_blob_digest) REFERENCES public.blobs (digest);

ALTER TABLE public.manifests
    ADD CONSTRAINT fk_manifests_configuration_media_type_id_media_types FOREIGN KEY (configuration_media_type_id) REFERENCES public.media_types (id);

ALTER TABLE public.manifests
    ADD CONSTRAINT fk_manifests_media_type_id_media_types FOREIGN KEY (media_type_id) REFERENCES public.media_types (id);

ALTER TABLE public.manifests
    ADD CONSTRAINT fk_manifests_top_lvl_nmespace_id_and_repository_id_repositories FOREIGN KEY (top_level_namespace_id, repository_id) REFERENCES public.repositories (top_level_namespace_id, id) ON DELETE CASCADE;

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT fk_repositories_top_level_namespace_id_top_level_namespaces FOREIGN KEY (top_level_namespace_id) REFERENCES public.top_level_namespaces (id) ON DELETE CASCADE;

ALTER TABLE public.repository_blobs
    ADD CONSTRAINT fk_repository_blobs_blob_digest_blobs FOREIGN KEY (blob_digest) REFERENCES public.blobs (digest) ON DELETE CASCADE;

ALTER TABLE public.repository_blobs
    ADD CONSTRAINT fk_repository_blobs_top_lvl_nmspc_id_and_rpstry_id_repositories FOREIGN KEY (top_level_namespace_id, repository_id) REFERENCES public.repositories (top_level_namespace_id, id) ON DELETE CASCADE;

ALTER TABLE public.tags
    ADD CONSTRAINT fk_tags_repository_id_and_manifest_id_manifests FOREIGN KEY (top_level_namespace_id, repository_id, manifest_id) REFERENCES public.manifests (top_level_namespace_id, repository_id, id) ON DELETE CASCADE;


INSERT INTO "gc_manifest_review_queue"("top_level_namespace_id", "repository_id", "manifest_id", "review_after", "review_count", "created_at", "event")
VALUES (1, 4, 7, E'2020-04-03 18:45:04.470711+00', 2, E'2020-04-02 18:45:04.470711+00', 'manifest_upload'),
       (1, 4, 9, E'9999-12-31 23:59:59.999999+00', 0, E'9999-12-30 23:59:59.999999+00', 'manifest_delete'),
       (1, 4, 4, E'2020-06-11 09:11:23.655121+00', 0, E'2020-06-10 09:11:23.655121+00', 'manifest_list_delete'),
       (1, 3, 1, E'2020-03-03 17:50:26.461745+00', 0, E'2020-03-02 17:50:26.461745+00', 'tag_switch');
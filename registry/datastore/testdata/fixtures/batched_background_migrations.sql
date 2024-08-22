INSERT INTO "batched_background_migrations"("id", "name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name", "created_at", "updated_at")
VALUES (1, 'CopyMediaTypesIDToNewIDColumn', 1, 100, 20, 2, 'CopyMediaTypesIDToNewIDColumn', 'public.media_types', 'id','2024-05-02 16:39:06.421215+00', NULL),
       (2, 'CopyBlobIDToNewIDColumn', 5, 10, 1, 1, 'CopyBlobIDToNewIDColumn', 'public.blobs', 'id','2024-06-02 16:39:06.421215+00', '2024-06-02 16:39:06.421215+00'),
       (3, 'CopyRepositoryIDToNewIDColumn', 1, 16, 1, 1, 'CopyRepositoryIDToNewIDColumn', 'public.repositories', 'id','2024-06-02 16:39:06.421215+00', '2024-06-02 16:39:06.421215+00');

INSERT INTO "tags"("id", "top_level_namespace_id", "name", "repository_id", "manifest_id", "created_at", "updated_at")
VALUES (1, 1, E'1.0.0', 3, 1, E'2020-03-02 17:57:43.283783+00', NULL),
       (2, 1, E'2.0.0', 3, 2, E'2020-03-02 17:57:44.283783+00', NULL),
       (3, 1, E'latest', 3, 2, E'2020-03-02 17:57:45.283783+00', E'2020-03-02 17:57:53.029514+00'),
       (4, 1, E'1.0.0', 4, 3, E'2020-03-02 17:57:46.283783+00', NULL),
       (5, 1, E'stable-9ede8db0', 4, 3, E'2020-03-02 17:57:47.283783+00', NULL),
       (6, 1, E'stable-91ac07a9', 4, 4, E'2020-04-15 09:47:26.461413+00', NULL),
       (7, 1, E'0.2.0', 3, 6, E'2020-04-15 09:47:26.461413+00', NULL),
       (8, 1, E'rc2', 4, 7, E'2020-04-15 09:47:26.461413+00', NULL),
       (9, 3, E'a', 10, 12, E'2021-11-24 11:36:05.210908+00', NULL),
       (10, 3, E'b', 10, 13, E'2021-11-24 11:38:57.313230+00', NULL),
       (11, 3, E'c', 10, 16, E'2021-11-24 11:49:45.008435+00', NULL),
       (12, 3, E'd', 10, 18, E'2021-11-24 11:59:13.421427+00', NULL),
       (13, 3, E'e', 11, 19, E'2022-02-22 11:59:13.421427+00', NULL),
       (14, 3, E'f', 13, 20, E'2022-02-22 11:59:13.421427+00', NULL),
       (15, 3, E'h', 14, 23, E'2022-02-22 11:59:13.421427+00', NULL),
       (16, 3, E'i', 9, 24, E'2022-02-22 12:01:05.114178+00', NULL),
       (17, 3, E'e', 10, 18, E'2021-11-24 11:59:13.421427+00', NULL),
       (18, 3, E'f', 10, 18, E'2021-11-24 11:59:13.421427+00', NULL),
       (19, 4, E'latest', 16, 27, E'2023-01-01 00:00:01.000000+00', E'2023-04-30 00:00:01.000000+00'),
       (20, 4, E'aaaa', 16, 25, E'2023-01-01 00:00:01.000000+00', NULL),
       (21, 4, E'bbbb', 16, 26, E'2023-02-01 00:00:01.000000+00', NULL),
       (22, 4, E'cccc', 16, 26, E'2023-03-01 00:00:01.000000+00', NULL),
       (23, 4, E'dddd', 16, 25, E'2023-04-01 00:00:01.000000+00', E'2023-04-30 00:00:01.000000+00'),
       (24, 4, E'ffff', 16, 27, E'2023-05-31 00:00:01.000000+00', NULL),
       (25, 4, E'eeee', 16, 27, E'2023-06-30 00:00:01.000000+00', NULL);
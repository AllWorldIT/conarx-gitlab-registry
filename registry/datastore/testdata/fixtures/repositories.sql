INSERT INTO "repositories"("id", "top_level_namespace_id", "name", "path", "parent_id", "created_at")
VALUES (1, 1, 'gitlab-org', 'gitlab-org', NULL, '2020-03-02 17:47:39.849864+00'),
       (2, 1, 'gitlab-test', 'gitlab-org/gitlab-test', 1, '2020-03-02 17:47:40.866312+00'),
       (3, 1, 'backend', 'gitlab-org/gitlab-test/backend', 2, '2020-03-02 17:42:12.566212+00'),
       (4, 1, 'frontend', 'gitlab-org/gitlab-test/frontend', 2, '2020-03-02 17:43:39.476421+00'),
       (5, 2, 'a-test-group', 'a-test-group', NULL, '2020-06-08 16:01:39.476421+00'),
       (6, 2, 'foo', 'a-test-group/foo', 5, '2020-06-08 16:01:39.476421+00'),
       (7, 2, 'bar', 'a-test-group/bar', 5, '2020-06-08 16:01:39.476421+00'),
       (8, 3, 'usage-group', 'usage-group', NULL, '2021-11-24 11:36:04.692846+00'),
       (9, 3, 'sub-group-1', 'usage-group/sub-group-1', 8, '2021-11-24 11:36:04.692846+00'),
       (10, 3, 'repository-1', 'usage-group/sub-group-1/repository-1', 9, '2021-11-24 11:36:04.692846+00'),
       (11, 3, 'repository-2', 'usage-group/sub-group-1/repository-2', 9, '2022-02-22 11:12:43.561123+00'),
       (12, 3, 'sub-group-2', 'usage-group/sub-group-2', 8, '2022-02-22 11:33:12.312211+00'),
       (13, 3, 'repository-1', 'usage-group/sub-group-2/repository-1', 12, '2022-02-22 11:33:12.434732+00'),
       (14, 3, 'sub-repository-1', 'usage-group/sub-group-2/repository-1/sub-repository-1', 13, '2022-02-22 11:33:12.434732+00'),
       (15, 4, 'usage-group-2', 'usage-group-2', NULL, '2022-02-22 15:36:04.692846+00'),
       (16, 4, 'project-1', 'usage-group-2/sub-group-1/project-1', 15, '2022-02-22 15:36:04.692846+00');


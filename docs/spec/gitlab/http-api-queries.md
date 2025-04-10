## HTTP API Queries

### Common Queries

Here we list queries used across several operations and refer to them from each operation to avoid repetitions.

#### Check if repository with a given path exists and grab its ID

```sql
SELECT
    id
FROM
    repositories
WHERE
    path = $1;
```

#### Find or create repository by path

This is a safe way to find or create a repository by path for highly concurrent write operations, namely blob and manifest uploads.

1. We start by finding or creating the namespace by `name`. This is the first portion of the path, e.g. `a` for a path of `a/b/c`:

   First we search for the target namespace:

   ```sql
   SELECT
        id,
        name,
        created_at,
        updated_at
    FROM
        top_level_namespaces
    WHERE
        name = $1;
   ```

   If it exists then we return, otherwise we do an upsert to create the namespace:

   ```sql
   INSERT INTO top_level_namespaces (name)
       VALUES ($1)
   ON CONFLICT (name)
       DO NOTHING
   RETURNING
       id, created_at;
   ```

   If the result set from the query above has no rows, then we know another concurrent process managed to create it and
we can find it by name:

   ```sql
   SELECT
       id,
       name,
       created_at,
       updated_at
   FROM
       top_level_namespaces
   WHERE
       name = $1;
   ```

1. With the namespace in hand, we create or find all parent repositories, including the root one (namespace). For a path of `a/b/c`, we therefore create repositories `a` and `b`, in this order, making sure to link them together through `parent_id`:

   First we search for the target repository:

   ```sql
   SELECT
     id,
     top_level_namespace_id,
     name,
     path,
     parent_id,
     created_at,
     updated_at
   FROM
     repositories
   WHERE
     path = $1;
   ```

   If it exists then we return, otherwise we do an upsert to create the repository:

   ```sql
   INSERT INTO repositories (top_level_namespace_id, name, path, parent_id)
       VALUES ($1, $2, $3, $4)
   ON CONFLICT (path)
       DO NOTHING
   RETURNING
       id, created_at;
   ```

   If the result set from each of the queries above has no rows, then we know the corresponding parent repository already exists and we can [find it by path](#check-if-repository-with-a-given-path-exists-and-grab-its-id).

1. Finally, we create the leaf repository, e.g., `c` for a path of `a/b/c`, linking it to `b`. This is done as described in (2).

#### Find manifest by digest in repository

```sql
SELECT
    m.id,
    m.top_level_namespace_id,
    m.repository_id,
    m.schema_version,
    mt.media_type,
    at.media_type as artifact_type,
    encode(m.digest, 'hex') as digest,
    m.payload,
    mtc.media_type as configuration_media_type,
    encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
    m.configuration_payload,
    m.subject_id,
    m.created_at
FROM
    manifests AS m
    JOIN media_types AS mt ON mt.id = m.media_type_id
    LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
    LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
WHERE
    m.top_level_namespace_id = $1
    AND m.repository_id = $2
    AND m.digest = decode($3, 'hex');
```

#### Check if manifest exists in repository

```sql
SELECT
    EXISTS (
        SELECT
            1
        FROM
            manifests
        WHERE
            top_level_namespace_id = $1
            AND repository_id = $2
            AND digest = decode($3, 'hex'));
```

#### Find blob by digest in repository

```sql
SELECT
    mt.media_type,
    encode(b.digest, 'hex') as digest,
    b.size,
    b.created_at
FROM
    blobs AS b
    JOIN media_types AS mt ON mt.id = b.media_type_id
    JOIN repository_blobs AS rb ON rb.blob_digest = b.digest
WHERE
    rb.top_level_namespace_id = $1
    AND rb.repository_id = $2
    AND b.digest = decode($3, 'hex');
```

#### Check if blob exists in repository

```sql
SELECT
    EXISTS (
        SELECT
            1
        FROM
            repository_blobs
        WHERE
            top_level_namespace_id = $1
            AND repository_id = $2
            AND blob_digest = decode($3, 'hex'));
```

#### Link blob to repository

This operation is idempotent.

```sql
INSERT INTO repository_blobs (top_level_namespace_id, repository_id, blob_digest)
    VALUES ($1, $2, decode($3, 'hex'))
ON CONFLICT (top_level_namespace_id, repository_id, blob_digest)
    DO NOTHING;
```

### Repositories

#### List

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#get-catalog)

```plaintext
GET /v2/_catalog 
```

1. Find all non empty repositories (with at least one manifest), lexicographically paginated and sorted by `path`:

```sql
SELECT
    r.id,
    r.top_level_namespace_id,
    r.name,
    r.path,
    r.parent_id,
    r.created_at,
    r.updated_at
FROM
    repositories AS r
WHERE
    EXISTS (
        SELECT
        FROM
            manifests AS m
        WHERE
            m.top_level_namespace_id = r.top_level_namespace_id
            AND m.repository_id = r.id) -- ignore repositories that have no manifests (empty)
    AND r.path > $1 -- pagination marker (lexicographic)
ORDER BY
    r.path
LIMIT $2; -- pagination limit
```

### Blobs

#### Pull

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#get-blob)

```plaintext
GET /v2/<name>/blobs/<digest>
```

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);
1. [Find blob with digest `<digest>` in repository `<name>`](#find-blob-by-digest-in-repository).

#### Check existence

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#existing-layers)

```plaintext
HEAD /v2/<name>/blobs/<digest>
```

Same as for pull operation. Although we're just checking for existence, the HTTP response includes headers with metadata, so we need to retrieve it from the database.

#### Push

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#put-blob-upload)

```plaintext
PUT /v2/<name>/blobs/uploads/<uuid>?digest=<digest>
```

1. [Find or create repository(ies) with `path` `<name>`](#find-or-create-repository-by-path);

1. "*Create or find*" blob with digest `<digest>` in repository `<name>`. We avoid a "*find or create*" because it's prone to race conditions on inserts and this is a concurrent operation:

   ```sql
   INSERT INTO blobs (digest, media_type_id, size)
       VALUES (decode($1, 'hex'), $2, $3)
   ON CONFLICT (digest)
       DO NOTHING
   RETURNING
       created_at;
   ```

   If the resultset is empty, we know the blob already exists and we [find it by digest](#find-blob-by-digest-in-repository). Otherwise, we get the attributes initialized by the database during the insert and proceed.

1. [Link blob with digest `<digest>` to repository `<name>`](#link-blob-to-repository).

#### Cross repository mount

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#mount-blob)

```plaintext
POST /v2/<name>/blobs/uploads/?mount=<digest>&from=<repository name> 
```

1. [Find or create repository(ies) with `path` `<name>`](#find-or-create-repository-by-path);
1. [Check if *source* repository with `path` `<repository name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);
1. [Check if blob with digest `<digest>` exists and is linked to the *source* `<repository name>` repository](#check-if-blob-exists-in-repository);
1. [Link blob with digest `<digest>` to *target* `<name>` repository](#link-blob-to-repository).

#### Delete blob link

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#delete-blob)

```plaintext
DELETE /v2/<name>/blobs/<digest> 
```

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);

1. Delete link for blob with digest `<digest>` in repository `<name>`:

   ```sql
   DELETE FROM repository_blobs
   WHERE top_level_namespace_id = $1
       AND repository_id = $2
       AND blob_digest = decode($3, 'hex');
   ```

   If the query affected no rows we know the blob link does not exist and raise the corresponding error. This avoids the need for a separate preceding `SELECT` to find if the link exists.

### Manifests

#### Pull

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#get-manifest)

```plaintext
GET /v2/<name>/manifests/<reference>
```

A manifest can be pulled by digest or tag.

##### By digest

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);
1. [Find manifest with digest `<reference>` in repository `<name>`](#find-manifest-by-digest-in-repository);

##### By tag

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);

1. Find manifest with tag name `<reference>`  within repository `<name>`:

   ```sql
   SELECT
       m.id,
       m.top_level_namespace_id,
       m.repository_id,
       m.created_at,
       m.schema_version,
       mt.media_type,
       at.media_type as artifact_type,
       encode(m.digest, 'hex') as digest,
       m.payload,
       mtc.media_type as configuration_media_type,
       encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
       m.configuration_payload,
       m.subject_id,
       m.created_at
   FROM
       manifests AS m
       JOIN media_types AS mt ON mt.id = m.media_type_id
       LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
       LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
       JOIN tags AS t ON t.top_level_namespace_id = m.top_level_namespace_id
            AND t.repository_id = m.repository_id
            AND t.manifest_id = m.id
   WHERE
       m.top_level_namespace_id = $1
       AND m.repository_id = $2
       AND t.name = $3;
   ```

#### Check existence

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#existing-manifests)

```plaintext
HEAD /v2/<name>/manifests/<reference>
```

Same as for pull operation. Although we're just checking for existence, the HTTP response includes headers with metadata, so we need to retrieve it from the database.

#### Push

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#put-manifest)

```plaintext
PUT /v2/<name>/manifests/<reference> 
```

A manifest can be either an atomic/indivisible manifest or a manifest list (e.g. multi-arch image). Regardless, a manifest can be either pushed by digest (untagged) or by tag (tagged).

##### "Atomic" manifests

###### By digest

1. [Find or create repository(ies) with `path` `<name>`](#find-or-create-repository-by-path)

1. For each referenced artifact in the manifest payload (configuration, layer and/or other manifest):

   - Configuration or layer: [Check if blob exists in repository `<name>`](#check-if-blob-exists-in-repository);

   - Manifest: [Check if manifest exists in repository `<name>`](#check-if-manifest-exists-in-repository).

1. "*Create or find*" manifest in repository `<name>`. We avoid a "*find or create*" because it's prone to race conditions on inserts and this is a concurrent operation:

   ```sql
   INSERT INTO manifests (top_level_namespace_id, repository_id, schema_version, media_type_id, digest, payload, configuration_payload, configuration_blob_digest)
       VALUES ($1, $2, $3, $4, decode($5, 'hex'), $6, $7, decode($8, 'hex'))
   ON CONFLICT (top_level_namespace_id, repository_id, digest)
       DO NOTHING
   RETURNING
       id, created_at;
   ```

   If the resultset is empty, we know the manifest already exists and [find it by digest](#find-manifest-by-digest-in-repository). Otherwise, we get the attributes initialized by the database during the insert and proceed.

1. For each layer in the manifest payload do:

   1. [Check if blob exists in repository `<name>`](#check-if-blob-exists-in-repository);

   1. Create layer record. It does nothing if already exists:

      ```sql
      INSERT INTO layers (top_level_namespace_id, repository_id, manifest_id, digest, size, media_type_id)
          VALUES ($1, $2, $3, decode($4, 'hex'), $5, $6)
      ON CONFLICT (top_level_namespace_id, repository_id, manifest_id, digest)
          DO NOTHING;
      ```

###### By tag

1. Same steps as by digest;

1. Upsert tag with name `<reference>` in repository `<name>`. A tag with name `<reference>` may: not exist; already exist and point to the same manifest; already exist but point to a different manifest.

   If the tag doesn't exist we insert it, if the tag already exists we update it, but only if the current manifest that it points to is different (to avoid "empty" updates that may trigger unwanted/unnecessary actions in the database):

   ```sql
   INSERT INTO tags (top_level_namespace_id, repository_id, manifest_id, name)
       VALUES ($1, $2, $3, $4)
   ON CONFLICT (top_level_namespace_id, repository_id, name)
       DO UPDATE SET
           manifest_id = EXCLUDED.manifest_id, updated_at = now()
       WHERE
           tags.manifest_id <> excluded.manifest_id -- only update if target manifest differs
   RETURNING
       id, created_at, updated_at;
   ```

##### Manifest lists

###### By digest

1. [Find or create repository(ies) with `path` `<name>`](#find-or-create-repository-by-path);

1. For each manifest referenced in the list, [check if manifest exists in repository `<name>`](#check-if-manifest-exists-in-repository);

1. "*Create or find*" manifest list in repository `<name>`. We avoid a "*find or create*" because it's prone to race conditions on inserts and this is a concurrent operation:

   ```sql
   INSERT INTO manifests (top_level_namespace_id, repository_id, schema_version, media_type_id, digest, payload, configuration_payload, configuration_blob_digest)
       VALUES ($1, $2, $3, $4, decode($5, 'hex'), $6, $7, decode($8, 'hex'))
   ON CONFLICT (top_level_namespace_id, repository_id, digest)
       DO NOTHING
   RETURNING
       id, created_at;
   ```

   If the resultset is empty, we know the manifest list already exists and [find it by digest](#find-manifest-by-digest-in-repository). Otherwise, we get the attributes initialized by the database during the insert and proceed.

1. Create a relationship record for each manifest referenced in the manifest list payload, where `parent_id` is the manifest list ID and `child_id` is the referenced manifest ID (bulk insert). Do nothing if relationship already exists:

   ```sql
   INSERT INTO manifest_references (top_level_namespace_id, repository_id, parent_id, child_id)
       VALUES ($1, $2, $3, $4)
   ON CONFLICT (top_level_namespace_id, repository_id, parent_id, child_id)
       DO NOTHING;
    ```

###### By tag

1. Same steps as by digest;
1. Upsert tag with name `<reference>` like it's done for atomic manifests.

#### Delete

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#delete-manifest)

```plaintext
DELETE /v2/<name>/manifests/<reference> 
```

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);

1. Delete manifest with digest `<reference>` in repository `<name>`:

   ```sql
   DELETE FROM manifests
   WHERE top_level_namespace_id = $1
       AND repository_id = $2
       AND digest = decode($3, 'hex');
   ```

   If the query affected no rows we know the manifest does not exist and raise the corresponding error. This avoids the need for a separate preceding `SELECT` to find if the manifest exists.

### Tags

#### List

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#get-tags)

```plaintext
GET /v2/<name>/tags/list 
```

1. Find all tags in repository `<name>`, lexicographically paginated and sorted by tag name:

   ```sql
   SELECT
       id,
       top_level_namespace_id,
       repository_id,
       manifest_id,
       name,
       created_at,
       updated_at
   FROM
       tags
   WHERE
       top_level_namespace_id = $1
       AND repository_id = $2
       AND name > $3 -- pagination marker (lexicographic)
   ORDER BY
       name
   LIMIT $4; -- pagination limit
   ```

#### Delete

[API docs](https://gitlab.com/gitlab-org/container-registry/-/blob/67bf50f4358c845d3e93a7bfd1318afb7c19786b/docs/spec/api.md#delete-tags)

```plaintext
DELETE /v2/<name>/tags/reference/<reference> 
```

1. [Check if repository with `path` `<name>` exists and grab its ID](#check-if-repository-with-a-given-path-exists-and-grab-its-id);

1. Delete tag `<reference>` in repository `<name>`:

   ```sql
   DELETE FROM tags
   WHERE top_level_namespace_id = $1
       AND repository_id = $2
       AND name = $3;
   ```

   If the resultset has no rows we know the tag does not exist and raise the corresponding error. This avoids the need for a separate preceding `SELECT` to find if the tag exists.

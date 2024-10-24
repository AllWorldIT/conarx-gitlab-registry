# Authentication Flow

This document illustrates the flow of API requests for the Docker login command, as well as for push/pull commands with and without being logged in.

Although here we use GitLab.com and Google Could Storage as example, the request flow should be the identical for self-managed instances and other storage backends.

## Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  C->>C: docker login registry.gitlab.com
  Note right of C: Ask user for crendentials and encode them in base64, using the format `username:password`
  C->>R: GET /v2/
  R->>C: 401 Unauthorized<br>Www-Authenticate: Bearer realm="https://gitlab.com/jwt/auth",service="container_registry"
  C->>G: GET /jwt/auth?account=<username>&client_id=docker&offline_token=true&service=container_registry
  Note right of C: This is only for authentication, not authorization (note `offline_token=true`)
  G->>C: 200 OK<br>{"token": "eyJ..."}
  C->>R: GET /v2/<br>Authorization: Bearer eyJ...
  R->>C: 200 OK
```

## Pull

### Private Repository

#### Without Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  C->>C: docker pull registry.gitlab.com/mygroup/myproj:latest
  C->>R: GET /v2/
  R->>C: 401 Unauthorized
  C->>G: GET /jwt/auth?scope=repository:mygroup/myproj:pull&service=container_registry
  Note right of C: The `Authorization` header is not set in this request, neither there is an `account=<username>` query parameter
  G->>C: 403 Forbidden
  C->>C: Error response from daemon: denied: access forbidden
```

#### With Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  participant S as GCS
  C->>C: docker pull registry.gitlab.com/mygroup/myproj:latest
  C->>R: GET /v2/
  Note right of C: The `Authorization` header is not set in this request
  R->>C: 401 Unauthorized
  Note right of C: The client asks for authorization to pull from the target repository
  C->>G: GET /jwt/auth?account=<username>&scope=repository:mygroup/myproj:pull&service=container_registry<br>Authorization:    Basic amR... (base 64 encoded `username:password`)
  G->>C: 200 OK<br>{"token": "eyJ..."}
  C->>R: GET registry.gitlab.com/v2/mygroup/myproj/manifests/latest<br>Authorization: Bearer eyJ...
  R->>S: GET /<bucket>/docker/registry/v2/blobs/sha256/<manifest digest>
  Note left of R: Unlike configs and layers, manifests are served directly by the registry for content negotiation
  R->>C: 200 OK<br>{"schemaVersion": ...}
  C->>R: GET registry.gitlab.com/v2/mygroup/myproj/blobs/<config digest><br>Authorization: Bearer eyJ...
  R->>S: HEAD /<bucket>/docker/registry/v2/blobs/sha256/<config digest>
  R->>R: Build pre-signed URL
  R->>C: 307 Temporary Redirect<br>Location: https://storage.googleapis.com/<bucket>/docker/registry/v2/blobs/sha256/<config digest>
  C->>S: GET https://storage.googleapis.com/<bucket>/docker/registry/v2/blobs/sha256/<config digest>
  loop For each layer in manifest
    C->>R: GET registry.gitlab.com/v2/mygroup/myproj/blobs/<layer digest><br>Authorization: Bearer eyJ...
    R->>S: HEAD /<bucket>/docker/registry/v2/blobs/sha256/<layer digest>
    R->>R: Build pre-signed URL
    R->>C: 307 Temporary Redirect<br>Location: https://storage.googleapis.com/<bucket>/docker/registry/v2/blobs/sha256/<layer digest>
    C->>S: GET https://storage.googleapis.com/<bucket>/docker/registry/v2/blobs/sha256/<layer digest>
  end
```

### Public Repository

#### Without Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  participant S as GCS
  C->>C: docker pull registry.gitlab.com/mygroup/myproj:latest
  C->>R: GET /v2/
  Note right of C: The `Authorization` header is not set in this request
  R->>C: 401 Unauthorized<br>Www-Authenticate: Bearer realm="https://gitlab.com/jwt/auth",service="container_registry"
  C->>G: GET /jwt/auth?scope=repository:mygroup/myproj:pull&service=container_registry
  Note right of C: The `Authorization` header is not set in this request, neither there is an `account=<username>` query parameter
  G->>C: 200 OK<br>{"token": "eyJ..."}
  Note right of C: Same as steps 6 to 18 for pulling private images with login
```

#### With Login

Same steps as without login, but here the request number 4 includes an `account=<username>` query parameter.

## Push

### Private Repository

#### Without Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  C->>C: docker push registry.gitlab.com/mygroup/myproj:2.0.0
  C->>R: GET /v2/
  R->>C: 401 Unauthorized
  C->>G: GET /jwt/auth?scope=repository:mygroup/myproj:push,pull&service=container_registry
  Note right of C: The `Authorization` header is not set in this request, neither there is an `account=<username>` query parameter
  G->>C: 403 Forbidden
  C->>G: Retry
  G->>C: 403 Forbidden
  C->>C: denied: access forbidden
```

#### With Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  participant S as GCS
  C->>C: docker push registry.gitlab.com/mygroup/myproj:2.0.0
  C->>R: GET /v2/
  Note right of C: The `Authorization` header is not set in this request
  R->>C: 401 Unauthorized
  C->>G: GET /jwt/auth?account=<username>&scope=repository:mygroup/myproj:push,pull&service=container_registry<br>Authorization:    Basic amR... (base 64 encoded `username:password`)
  Note right of C: Now the client asks for authorization to push to the target repository
  G->>C: 200 OK<br>{"token": "eyJ..."}
  Note right of C: Requests continue as described in *. All requests to the registry include an `Authorization` header with the token received in step 5. 
```

`*` See the detailed [push request flow](push-pull-request-flow.md#push)

### Public Repository

#### Without Login

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant G as GitLab Rails
  C->>C: docker push registry.gitlab.com/mygroup/myproj:2.0.0
  C->>R: GET /v2/
  R->>C: 401 Unauthorized
  C->>G: GET /jwt/auth?scope=repository:mygroup/myproj:push,pull&service=container_registry
  Note right of C: The `Authorization` header is not set in this request, neither there is an `account=<username>` query parameter
  G->>C: 200 OK<br>{"token": "eyJ..."}
  Note right of C: Requests continue as described in *. All requests to the registry include an `Authorization` header with the token received in step 5. All POST requests will fail with `401 Unauthorized`.
  C->>C: denied: requested access to the resource is denied
```

`*` See the detailed [push request flow](push-pull-request-flow.md#push)

#### With Login

Same steps as for pushing to a private repository with login.


# Request Flow

This document illustrates the flow of the API request for GitLab Rails to rename a project with container repositories.

This flow works under the assumption that all Authorization tokens referenced in the diagram have been issued to GitLab Rails with full sub-repository pull scopes and base-repository push & pull scopes.

**Note** : When making a request to the rename API GitLab Rails must pass the project path in place of the repository (base) `<path>` parameter in the request. This is because some repositories may not have a base repository (i.e. a repository with the same exact path as the GitLab project) but may have sub repositories under the GitLab project path, which would still need to be renamed.


## Rename (Dry-run)

```mermaid
sequenceDiagram
  autonumber
  participant G as GitLab Rails
  participant R as GitLab Container Registry
  participant RR as Container Registry Redis
  participant P as Container Registry Postgres
  G->>R: PATCH /gitlab/v1/repositories/my-group/my-sub-group/old-name/?dry_run=true Body:{name:"new-name"}<br>Authorization: Bearer eyJ...
  R->>RR: Check if a lease exists on the authorization token's "project_path" claims
  alt lease found for "project_path" claim: "my-group/my-sub-group/old-name" (signifies the path is undergoing a rename)
    R->>G: 409 Conflict
  else lease not found: "my-group/my-sub-group/old-name" is not undergoing a rename
    R->>R: Check any authorization token "project_path" claims matches repository base path in request path parameter
    alt The requested path parameter : "my-group/my-sub-group/old-name" <br>does not match any of the token's "project_path" claims
      R->>G: 400 Bad Request
    else The requested project path parameter : "my-group/my-sub-group/old-name" <br> matches one of the token's "project_path" claim
        R->>RR: Check if a repository lease exists on the new name "new-name"
        alt There is conflicting repository lease <br>(i.e another existing base-repository in the same namespace holds a lease for "new-name")
          R->>G: 409 Conflict
        else There are no conflicting repository leases
          R->>P: Count number of repositories that start with "my-group/my-sub-group/old-name" <br> in database's repository table
          alt path contains more than 1000 sub-repos
            R->>G: 422 Unprocessable Entity
          else path contains 0 sub-repos
            R->>G: 404 Not Found
          else path contains less than 1000 (but greater than 0) sub-repos
            R->>RR: Retrieve the existing repository lease (if any)
            alt There is no existing repository lease for <br>"my-group/my-sub-group/old-name" to "my-group/my-sub-group/new-name"
              R->>RR: Create the necessary repository lease on "new-name" for 60 seconds
            else There is an existing repository lease for  <br>"my-group/my-sub-group/old-name" to "my-group/my-sub-group/new-name"
              R->>RR: Extend the existing repository lease TTL of "new-name" for 60 seconds (to allow time to successfully complete a rename)
            end
            alt Lease procurment/extension for "new-name" was successful
              R->>G: 202 Body:{ttl:"2009-11-10T23:00:00.005Z"} 
            else Lease procurment for "new-name" was unsuccessful
                R->>G: 500 Internal Error
            end
          end
        end
    end
  end
```

## Rename

```mermaid
sequenceDiagram
  autonumber
  participant G as GitLab Rails
  participant R as GitLab Container Registry
  participant RR as Container Registry Redis
  participant P as Container Registry Postgres
  G->>R: PATCH /gitlab/v1/repositories/my-group/my-sub-group/old-name/?dry_run=false Body:{name:"new-name"}<br>Authorization: Bearer eyJ...
  R->>RR: Check if a lease exists on the authorization token's "project_path" claims
  alt lease found for "project_path" claim: "my-group/my-sub-group/old-name" (signifies the path is undergoing a rename)
    R->>G: 409 Conflict
  else lease not found: "my-group/my-sub-group/old-name" is not undergoing a rename
    R->>R: Check any authorization token "project_path" claims matches repository base path in request path parameter
    alt The requested path parameter : "my-group/my-sub-group/old-name" <br>does not match any of the token's "project_path" claims
      R->>G: 400 Bad Request
    else The requested project path parameter : "my-group/my-sub-group/old-name" <br> matches one of the token's "project_path" claim
        R->>RR: Check if a repository lease exists on the new name "new-name"
        alt There is conflicting repository lease <br>(i.e another existing base-repository in the same namespace holds a lease for "new-name")
          R->>G: 409 Conflict
        else There are no conflicting repository leases
          R->>P: Count number of repositories that start with "my-group/my-sub-group/old-name" <br> in database's repository table
          alt path contains more than 1000 sub-repos
            R->>G: 422 Unprocessable Entity
          else path contains 0 sub-repos
            R->>G: 404 Not Found
          else path contains less than 1000 (but greater than 0) sub-repos
            R->>RR: Retrieve the existing repository lease (if any)
            alt There is no existing repository lease for <br>"my-group/my-sub-group/old-name" to "my-group/my-sub-group/new-name"
              R->>RR: Create the necessary repository lease on "new-name" for 60 seconds
            else There is an existing repository lease for  <br>"my-group/my-sub-group/old-name" to "my-group/my-sub-group/new-name"
              R->>RR: Extend the existing repository lease TTL of "new-name" for 60 seconds (to allow time to successfully complete a rename)
            end
            R->>RR: Procure (at most) a 6 seconds lease on the current project path to prevent writes to all repositories under path "my-group/my-sub-group/old-name" 
            R->>P: Execute Rename
            alt Rename was successful
              R->>G: 201 No Content
            else Rename was unsuccessful
              R->>G: 500 Internal Error
            end
          end
        end
    end
  end
```

## Ongoing Rename Effect On APIs That Write To Repositories

```mermaid
sequenceDiagram
  autonumber
  participant G as GitLab Rails
  participant R as GitLab Container Registry
  participant P as Container Registry Redis
  G->>R: DELETE/POST/PATCH/PUT /gitlab/v1/repositories/my-group/my-sub-group/old-name/... <br>Authorization: Bearer eyJ...
    R->>P: Check if a lease exists on the token's project_path claims
    alt lease found
      R->>G: 409 Conflict
    else lease not found
      R->>R: execute requested operation
      R->>G: Respond
    end
```

```mermaid
sequenceDiagram
  autonumber
  participant C as Docker Client
  participant R as GitLab Container Registry
  participant P as Container Registry Redis
  C->>R: POST/PATCH/PUT/DELETE /v2/my-group/my-sub-group/old-name/... <br>Authorization: Bearer eyJ...
    R->>P: Check if a lease exists on the token's project_path claims
    alt lease found
      R->>C: 409 Conflict
    else lease not found
      R->>R: execute requested operation
      R->>C: Respond
    end
```

package models

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/docker/distribution/registry/internal/migration"

	"github.com/opencontainers/go-digest"
)

// Payload implements sql/driver.Valuer interface, allowing pgx to use
// the PostgreSQL simple protocol.
type Payload json.RawMessage

// Value returns the payload serialized as a []byte.
func (p Payload) Value() (driver.Value, error) {
	return json.RawMessage(p).MarshalJSON()
}

// Namespace represents a root repository.
type Namespace struct {
	ID        int64
	Name      string
	CreatedAt time.Time
	UpdatedAt sql.NullTime
}

type Repository struct {
	ID              int64
	NamespaceID     int64
	Name            string
	Path            string
	ParentID        sql.NullInt64
	MigrationStatus migration.RepositoryStatus
	CreatedAt       time.Time
	UpdatedAt       sql.NullTime
}

// Repositories is a slice of Repository pointers.
type Repositories []*Repository

type Configuration struct {
	MediaType string
	Digest    digest.Digest
	// Payload is the JSON payload of a manifest configuration. For operational safety reasons,
	// a payload is only saved in this attribute if its size does not exceed a predefined
	// limit (see handlers.dbConfigSizeLimit).
	Payload Payload
}

type Manifest struct {
	ID            int64
	NamespaceID   int64
	RepositoryID  int64
	TotalSize     int64
	SchemaVersion int
	MediaType     string
	Digest        digest.Digest
	Payload       Payload
	Configuration *Configuration
	NonConformant bool
	// NonDistributableLayers identifies whether a manifest references foreign/non-distributable layers. For now, we are
	// not registering metadata about these layers, but we may wish to backfill that metadata in the future by parsing
	// the manifest payload.
	NonDistributableLayers bool
	CreatedAt              time.Time
}

// Manifests is a slice of Manifest pointers.
type Manifests []*Manifest

type Tag struct {
	ID           int64
	NamespaceID  int64
	Name         string
	RepositoryID int64
	ManifestID   int64
	CreatedAt    time.Time
	UpdatedAt    sql.NullTime
}

// Tags is a slice of Tag pointers.
type Tags []*Tag

type Blob struct {
	MediaType string
	Digest    digest.Digest
	Size      int64
	CreatedAt time.Time
}

// Blobs is a slice of Blob pointers.
type Blobs []*Blob

// GCBlobTask represents a row in the gc_blob_review_queue table.
type GCBlobTask struct {
	ReviewAfter time.Time
	ReviewCount int
	Digest      digest.Digest
	CreatedAt   time.Time
	Event       string
}

// GCConfigLink represents a row in the gc_blobs_configurations table.
type GCConfigLink struct {
	ID           int64
	NamespaceID  int64
	RepositoryID int64
	ManifestID   int64
	Digest       digest.Digest
}

// GCLayerLink represents a row in the gc_blobs_layers table.
type GCLayerLink struct {
	ID           int64
	NamespaceID  int64
	RepositoryID int64
	LayerID      int64
	Digest       digest.Digest
}

// GCManifestTask represents a row in the gc_manifest_review_queue table.
type GCManifestTask struct {
	NamespaceID  int64
	RepositoryID int64
	ManifestID   int64
	ReviewAfter  time.Time
	ReviewCount  int
	CreatedAt    time.Time
	Event        string
}

// GCReviewAfterDefault represents a row in the gc_review_after_defaults table.
type GCReviewAfterDefault struct {
	Event string
	Value time.Duration
}

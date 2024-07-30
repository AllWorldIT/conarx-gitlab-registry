package models

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"time"

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
	ID          int64
	NamespaceID int64
	Name        string
	Path        string
	ParentID    sql.NullInt64
	CreatedAt   time.Time
	UpdatedAt   sql.NullTime
	// This is a temporary attribute for the duration of https://gitlab.com/gitlab-org/container-registry/-/issues/570,
	// and is only here to allow us to test selects and inserts for soft-deleted repositories:
	DeletedAt sql.NullTime
	// The Size of the repository in bytes. The Size of a repository can be 0, so we use a pointer
	// to differentiate between a "0 byte" size repository and a repository that has nil size attribute
	// (i.e the attribute was not cached or was invalidated).
	// This value is not saved in the DB so we don't need to use a sql.NullInt64 type.
	Size *int64
	// LastPublishedAt is set to the timestamp of the latest published tag in the repository. This should equal to
	// MAX(GREATEST(created_at, updated_at)) among all tags.
	LastPublishedAt sql.NullTime
}

// IsTopLevel identifies whether a repository is a top-level repository or not.
func (r *Repository) IsTopLevel() bool {
	return !strings.Contains(r.Path, "/")
}

// TopLevelPathSegment returns the top-level path segment.
func (r *Repository) TopLevelPathSegment() string {
	return strings.Split(r.Path, "/")[0]
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
	ArtifactType  sql.NullString
	Digest        digest.Digest
	Payload       Payload
	Configuration *Configuration
	SubjectID     sql.NullInt64
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

// NullDigest is used to represent a digest.Digest that might be empty. Note that the value of Valid does not
// necessarily guarantee that the value of Digest is/is not a valid digest, only that it is/is not an empty string. So
// this is similar to sql.NullString, but embeds a digest.Digest instead of a plain string for convenience. It is the
// responsibility of the code that initializes this struct to guarantee that the value of Digest is a valid digest.
type NullDigest struct {
	Digest digest.Digest
	Valid  bool
}

// TagDetail is a virtual entity with no parallel on the database schema. This provides a set of attributes obtained
// by merging a Tag entity with the corresponding Manifest entity and the GET /gitlab/v1/<name>/tags/list API endpoint
// is its primary use case.
type TagDetail struct {
	Name         string
	Digest       digest.Digest
	ConfigDigest NullDigest
	MediaType    string
	Size         int64
	CreatedAt    time.Time
	UpdatedAt    sql.NullTime
	PublishedAt  time.Time
	Referrers    []TagReferrerDetail
}

type TagReferrerDetail struct {
	Digest       string
	ArtifactType string
}

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

// LeaseType defines the types of available leases on repositories
type LeaseType string

// RenameLease is a repository LeaseType for rename operations
var RenameLease LeaseType = "rename_lease"

// RepositoryLease represents a lease (on a new repository space) granted to an existing repository
type RepositoryLease struct {
	GrantedTo string
	Path      string
	Type      LeaseType
}

// BackgroundMigration is the representation of a BBM.
type BackgroundMigration struct {
	ID           int
	Name         string
	Status       BackgroundMigrationStatus
	StartID      int
	EndID        int
	BatchSize    int
	JobName      string
	TargetTable  string
	TargetColumn string
	ErrorCode    BBMErrorCode
}

// BackgroundMigrationJob is the representation of a BackgroundMigration Job.
type BackgroundMigrationJob struct {
	ID               int
	BBMID            int
	StartID          int
	EndID            int
	Status           BackgroundMigrationStatus
	Attempts         int
	JobName          string
	PaginationColumn string
	PaginationTable  string
	BatchSize        int
	ErrorCode        BBMErrorCode
}

// BackgroundMigrationStatus are the Background Migration and Background Migration job statuses as defined in:
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-background-migrations.md#batch-background-migration-bbm-creation
type BackgroundMigrationStatus int

const (
	BackgroundMigrationPaused BackgroundMigrationStatus = iota
	BackgroundMigrationActive
	BackgroundMigrationFinished
	BackgroundMigrationFailed
	BackgroundMigrationRunning
)

func (s BackgroundMigrationStatus) String() string {
	switch s {
	case BackgroundMigrationPaused:
		return "paused"
	case BackgroundMigrationActive:
		return "active"
	case BackgroundMigrationFinished:
		return "finished"
	case BackgroundMigrationFailed:
		return "failed"
	case BackgroundMigrationRunning:
		return "running"
	}
	return "unknown"
}

// BBMErrorCode represent the failure codes for Background Migration and Background Migration jobs as defined in:
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-background-migrations.md#asynchronous-execution-when-serving-requests-on-the-registry
type BBMErrorCode struct {
	sql.NullInt16
}

var (
	UnknownBBMErrorCode            = BBMErrorCode{sql.NullInt16{Int16: 0, Valid: true}}
	InvalidTableBBMErrCode         = BBMErrorCode{sql.NullInt16{Int16: 1, Valid: true}}
	InvalidColumnBBMErrCode        = BBMErrorCode{sql.NullInt16{Int16: 2, Valid: true}}
	InvalidJobSignatureBBMErrCode  = BBMErrorCode{sql.NullInt16{Int16: 3, Valid: true}}
	JobExceedsMaxAttemptBBMErrCode = BBMErrorCode{sql.NullInt16{Int16: 4, Valid: true}}
)

func (s BBMErrorCode) String() string {
	switch s.Int16 {
	default:
		return "unknown"
	case InvalidTableBBMErrCode.Int16:
		return "invalid_bbm_table"
	case InvalidColumnBBMErrCode.Int16:
		return "invalid_bbm_column"
	case InvalidJobSignatureBBMErrCode.Int16:
		return "invalid_job_signature"
	case JobExceedsMaxAttemptBBMErrCode.Int16:
		return "max_job_retry"
	}
}

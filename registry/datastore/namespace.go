package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
)

// NamespaceReader is the interface that defines read operations for a namespace store.
type NamespaceReader interface {
	FindByName(ctx context.Context, name string) (*models.Namespace, error)
}

// NamespaceWriter is the interface that defines write operations for a namespace store.
type NamespaceWriter interface {
	SafeFindOrCreate(ctx context.Context, r *models.Namespace) error
}

// NamespaceStore is the interface that a namespace store should conform to.
type NamespaceStore interface {
	NamespaceReader
	NamespaceWriter
}

// namespaceStore is the concrete implementation of a NamespaceStore.
type namespaceStore struct {
	// db can be either a *sql.DB or *sql.Tx
	db Queryer
}

// NewNamespaceStore builds a new repositoryStore.
func NewNamespaceStore(db Queryer) *namespaceStore {
	return &namespaceStore{db: db}
}

func scanFullNamespace(row *sql.Row) (*models.Namespace, error) {
	n := new(models.Namespace)

	if err := row.Scan(&n.ID, &n.Name, &n.CreatedAt, &n.UpdatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning namespace: %w", err)
		}
		return nil, nil
	}

	return n, nil
}

// FindByName finds a namespace by name.
func (s *namespaceStore) FindByName(ctx context.Context, name string) (*models.Namespace, error) {
	defer metrics.InstrumentQuery("namespace_find_by_name")()
	q := `SELECT
			id,
			name,
			created_at,
			updated_at
		FROM
			top_level_namespaces
		WHERE
			name = $1`
	row := s.db.QueryRowContext(ctx, q, name)

	return scanFullNamespace(row)
}

// SafeFindOrCreate provides a concurrency safe way to find or create a namespace record. This is optimized for the fact
// that 1) for the vast majority of requests the target namespace will already exist (low/moderate creation rate) and 2)
// we never delete namespace records from the database. This method works by 1) find namespace record 2) if found return
// otherwise perform an upsert (using createOrFind).
func (s *namespaceStore) SafeFindOrCreate(ctx context.Context, n *models.Namespace) error {
	defer metrics.InstrumentQuery("namespace_safe_find_or_create")()

	tmp, err := s.FindByName(ctx, n.Name)
	if err != nil {
		return err
	}
	if tmp != nil {
		*n = *tmp
		return nil
	}
	return s.createOrFind(ctx, n)
}

// createOrFind attempts to create a namespace. If the namespace already exists (same name) that record is loaded from
// the database into n. This is similar to a FindByName followed by a Create, but without being prone to race conditions
// on write operations between the corresponding read (FindByName) and write (Create) operations. Separate Find* and
// Create method calls should be preferred to this when race conditions are not a concern.
func (s *namespaceStore) createOrFind(ctx context.Context, n *models.Namespace) error {
	defer metrics.InstrumentQuery("namespace_create_or_find")()
	q := `INSERT INTO top_level_namespaces (name)
			VALUES ($1)
		ON CONFLICT (name)
			DO NOTHING
		RETURNING
			id, created_at`

	row := s.db.QueryRowContext(ctx, q, n.Name)
	if err := row.Scan(&n.ID, &n.CreatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("creating namespace: %w", err)
		}
		// if the result set has no rows, then the namespace already exists
		tmp, err := s.FindByName(ctx, n.Name)
		if err != nil {
			return err
		}
		*n = *tmp
	}

	return nil
}

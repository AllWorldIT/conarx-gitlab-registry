package metrics

import "github.com/docker/go-metrics"

const (
	// NamespacePrefix is the namespace of prometheus metrics
	NamespacePrefix = "registry"
)

// StorageNamespace is the prometheus namespace of blob/cache related operations
var StorageNamespace = metrics.NewNamespace(NamespacePrefix, "storage", nil)

package metrics

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestTagCount(t *testing.T) {
	TagCount(ImportTypePre, 10)
	TagCount(ImportTypeFinal, 9)

	var expected bytes.Buffer
	expected.WriteString(`
# HELP registry_migration_tag_counts A histogram of tag counts per repository (pre)import.
# TYPE registry_migration_tag_counts histogram
registry_migration_tag_counts_bucket{import_type="final",le="0"} 0
registry_migration_tag_counts_bucket{import_type="final",le="1"} 0
registry_migration_tag_counts_bucket{import_type="final",le="2"} 0
registry_migration_tag_counts_bucket{import_type="final",le="5"} 0
registry_migration_tag_counts_bucket{import_type="final",le="10"} 1
registry_migration_tag_counts_bucket{import_type="final",le="25"} 1
registry_migration_tag_counts_bucket{import_type="final",le="50"} 1
registry_migration_tag_counts_bucket{import_type="final",le="100"} 1
registry_migration_tag_counts_bucket{import_type="final",le="250"} 1
registry_migration_tag_counts_bucket{import_type="final",le="500"} 1
registry_migration_tag_counts_bucket{import_type="final",le="1000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="2000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="5000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="10000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="15000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="20000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="50000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="100000"} 1
registry_migration_tag_counts_bucket{import_type="final",le="+Inf"} 1
registry_migration_tag_counts_sum{import_type="final"} 9
registry_migration_tag_counts_count{import_type="final"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="0"} 0
registry_migration_tag_counts_bucket{import_type="pre",le="1"} 0
registry_migration_tag_counts_bucket{import_type="pre",le="2"} 0
registry_migration_tag_counts_bucket{import_type="pre",le="5"} 0
registry_migration_tag_counts_bucket{import_type="pre",le="10"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="25"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="50"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="100"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="250"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="500"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="1000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="2000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="5000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="10000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="15000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="20000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="50000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="100000"} 1
registry_migration_tag_counts_bucket{import_type="pre",le="+Inf"} 1
registry_migration_tag_counts_sum{import_type="pre"} 10
registry_migration_tag_counts_count{import_type="pre"} 1
`)
	CountsFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, tagCountName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, CountsFullName)
	require.NoError(t, err)
}

func TestLayerCount(t *testing.T) {
	LayerCount(1)
	LayerCount(100)

	var expected bytes.Buffer
	expected.WriteString(`
# HELP registry_migration_layer_counts A histogram of layer counts per (pre)imported manifest.
# TYPE registry_migration_layer_counts histogram
registry_migration_layer_counts_bucket{le="1"} 1
registry_migration_layer_counts_bucket{le="2"} 1
registry_migration_layer_counts_bucket{le="5"} 1
registry_migration_layer_counts_bucket{le="10"} 1
registry_migration_layer_counts_bucket{le="25"} 1
registry_migration_layer_counts_bucket{le="50"} 1
registry_migration_layer_counts_bucket{le="100"} 2
registry_migration_layer_counts_bucket{le="200"} 2
registry_migration_layer_counts_bucket{le="+Inf"} 2
registry_migration_layer_counts_sum 101
registry_migration_layer_counts_count 2
`)
	CountFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, layerCountName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, CountFullName)
	require.NoError(t, err)
}

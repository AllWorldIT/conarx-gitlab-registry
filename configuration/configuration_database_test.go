package configuration

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDatabase_Discovery_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  discovery:
    enabled: %s
`
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Database.Discovery.Enabled))
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DISCOVERY_ENABLED", tt, validator)
}

func TestDatabase_Discovery_Nameserver(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  discovery:
    enabled: true
    nameserver: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "sample.dns.name",
			want:  "sample.dns.name",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Database.Discovery.Nameserver)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DISCOVERY_NAMESERVER", tt, validator)
}

func TestDatabase_Discovery_Port(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  discovery:
    enabled: true
    port: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "5353",
			want:  "5353",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Database.Discovery.Port)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DISCOVERY_PORT", tt, validator)
}

func TestDatabase_Discovery_PrimaryRecord(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  discovery:
    enabled: true
    primaryrecord: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "valid.fqdn.record.",
			want:  "valid.fqdn.record.",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Database.Discovery.PrimaryRecord)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DISCOVERY_PRIMARYRECORD", tt, validator)
}

func TestParseDatabase_Discovery_TCP(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  discovery:
    enabled: true
    tcp: %s
`
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Database.Discovery.TCP))
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DISCOVERY_TCP", tt, validator)
}

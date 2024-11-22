package configuration

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type localConfiguration struct {
	Version Version `yaml:"version"`
	Log     *Log    `yaml:"log"`
}

type Log struct {
	Formatter string `yaml:"formatter,omitempty"`
}

var expectedConfig = localConfiguration{
	Version: "0.1",
	Log: &Log{
		Formatter: "json",
	},
}

func TestParserSuite(t *testing.T) {
	suite.Run(t, new(ParserSuite))
}

type ParserSuite struct {
	suite.Suite
}

func (suite *ParserSuite) TestParserOverwriteIninitializedPoiner() {
	config := localConfiguration{}

	os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	defer os.Unsetenv("REGISTRY_LOG_FORMATTER")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c any) (any, error) {
				return c, nil
			},
		},
	})

	err := p.Parse([]byte(`{version: "0.1", log: {formatter: "text"}}`), &config)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), expectedConfig, config)
}

func (suite *ParserSuite) TestParseOverwriteUnininitializedPoiner() {
	config := localConfiguration{}

	os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	defer os.Unsetenv("REGISTRY_LOG_FORMATTER")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c any) (any, error) {
				return c, nil
			},
		},
	})

	err := p.Parse([]byte(`{version: "0.1"}`), &config)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), expectedConfig, config)
}

package common

import (
	"fmt"

	"github.com/docker/distribution/registry/storage/driver/internal/parse"
)

const (
	ParamCredentialsType     = "credentialstype"
	CredentialsTypeSharedKey = "shared_key"

	ParamAccountName = "accountname"
	ParamAccountKey  = "accountkey"

	ParamContainer     = "container"
	ParamRealm         = "realm"
	ParamServiceURLKey = "serviceurl"
	ParamRootDirectory = "rootdirectory"

	ParamPoolInitialInterval = "apipoolinitialinterval"
	ParamPoolMaxInterval     = "apipoolmaxinterval"
	ParamPoolMaxElapsedTime  = "apipoolmaxelapsedtime"

	ParamTrimLegacyRootPrefix = "trimlegacyrootprefix"
	ParamLegacyRootPrefix     = "legacyrootprefix"

	// NOTE(prozlach): In theory debug logging could be enabled using
	// AZURE_SDK_GO_LOGGING env var, but in practice we want to have
	// fine-grained control over events that get logged, we want events to be
	// logged using logrus just like other drivers do and we want logging
	// configurable from within driver configuration.
	ParamDebugLog       = "debuglog"
	ParamDebugLogEvents = "debuglogevents"

	// NOTE(prozlach): Names of the variables are intentional, in order to
	// match those required by:
	// 		github.com/Azure/azure-sdk-for-go/sdk/azidentity.NewEnvironmentCredential()
	// which is implicitly called by:
	// 		github.com/Azure/azure-sdk-for-go/sdk/azidentity.NewDefaultAzureCredential()

	// NOTE(prozlach) the names environment variables are the same across
	// different driver versions.

	// EnvCredentialsType determines the type of credentials to use, determines
	// which env variables need to be set, must be one of:
	// * shared_key
	// nolint: gosec // Potential hardcoded credentials
	EnvCredentialsType = "AZURE_CREDENTIALS_TYPE"

	// Shared keys credentials:
	EnvAccountName = "AZURE_ACCOUNT_NAME"
	EnvAccountKey  = "AZURE_ACCOUNT_KEY"

	// When testing default azure credentials, we use CLI credentials which in
	// turn authenticates using a certificate

	// Common parameters:
	EnvContainer = "AZURE_CONTAINER"
	EnvRealm     = "AZURE_REALM"

	// Enables debug logging for Azure SDK. It is possible to filter the events
	// being printed - check the code in the `ParseParameters()` function,
	// where we parse the `common.ParamDebugLogEvents` parameter
	EnvDebugLog = "AZURE_DEBUGLOG"

	// specifies driver to use, one of: "azure", "azure_v2"
	EnvDriverVersion = "AZURE_DRIVER_VERSION"
)

// InferRootPrefixConfiguration determines when to use the azure legacy root prefix or not based on the storage driver configurations
func InferRootPrefixConfiguration(ac map[string]any) (useLegacyRootPrefix bool, err error) {
	// extract config values
	_, trimPrefixConfigExist := ac[ParamTrimLegacyRootPrefix]
	_, legacyPrefixConfigExist := ac[ParamLegacyRootPrefix]

	switch {
	// both parameters exist, check for conflict:
	case trimPrefixConfigExist && legacyPrefixConfigExist:
		// extract the boolean representation of each of the config parameters
		trimlegacyrootprefix, err := parse.Bool(ac, ParamTrimLegacyRootPrefix, true)
		if err != nil {
			return false, err
		}
		legacyrootprefix, err := parse.Bool(ac, ParamLegacyRootPrefix, true)
		if err != nil {
			return false, err
		}

		// conflict: user explicitly enabled legacyrootprefix but also enabled trimlegacyrootprefix or disabled both.
		if legacyrootprefix == trimlegacyrootprefix {
			err = fmt.Errorf("storage.azure.trimlegacyrootprefix' and  'storage.azure.trimlegacyrootprefix' can not both be %v", legacyrootprefix)
			return false, err
		}
		useLegacyRootPrefix = legacyrootprefix

	// both parameters do not exist; we will be using the default (i.e storage.azure.legacyrootprefix=false aka storage.azure.trimlegacyrootprefix=true):
	case !trimPrefixConfigExist && !legacyPrefixConfigExist:
		useLegacyRootPrefix = false

	// one config parameter does not exist, while the other does;
	// (in this case `legacyrootprefixExist` always takes precedence when it exists)
	case !trimPrefixConfigExist || !legacyPrefixConfigExist:
		if legacyPrefixConfigExist {
			// extract the boolean representation of each of the config parameters
			legacyrootprefix, err := parse.Bool(ac, ParamLegacyRootPrefix, true)
			if err != nil {
				return false, err
			}
			useLegacyRootPrefix = legacyrootprefix
		} else {
			// extract the boolean representation of each of the config parameters
			trimlegacyrootprefix, err := parse.Bool(ac, ParamTrimLegacyRootPrefix, true)
			if err != nil {
				return false, err
			}
			useLegacyRootPrefix = !trimlegacyrootprefix
		}
	}
	return useLegacyRootPrefix, err
}

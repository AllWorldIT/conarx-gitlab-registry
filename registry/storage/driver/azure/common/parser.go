package common

import (
	"fmt"

	"github.com/docker/distribution/registry/storage/driver/internal/parse"
)

const (
	ParamCredentialsType              = "credentials_type"
	CredentialsTypeSharedKey          = "shared_key"
	CredentialsTypeClientSecret       = "client_secret"
	CredentialsTypeDefaultCredentials = "default_credentials"

	ParamAccountName = "accountname"
	ParamAccountKey  = "accountkey"

	ParamTenantID = "tenant_id"
	ParamClientID = "client_id"
	ParamSecret   = "secret"

	ParamContainer     = "container"
	ParamRealm         = "realm"
	ParamServiceURLKey = "serviceurl"
	ParamRootDirectory = "rootdirectory"

	ParamPoolInitialInterval = "api_pool_initial_interval"
	ParamPoolMaxInterval     = "api_pool_max_interval"
	ParamPoolMaxElapsedTime  = "api_pool_max_elapsed_time"

	ParamMaxRetries      = "max_retries"
	ParamRetryTryTimeout = "retry_try_timeout"
	ParamRetryDelay      = "retry_delay"
	ParamMaxRetryDelay   = "max_retry_delay"

	ParamTrimLegacyRootPrefix = "trimlegacyrootprefix"
	ParamLegacyRootPrefix     = "legacyrootprefix"

	// NOTE(prozlach): In theory debug logging could be enabled using
	// AZURE_SDK_GO_LOGGING env var, but in practice we want to have
	// fine-grained control over events that get logged, we want events to be
	// logged using logrus just like other drivers do and we want logging
	// configurable from within driver configuration.
	ParamDebugLog       = "debug_log"
	ParamDebugLogEvents = "debug_log_events"

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
	// * client_secret
	// * default_credentials
	// nolint: gosec // Potential hardcoded credentials
	EnvCredentialsType = "AZURE_CREDENTIALS_TYPE"

	// Shared keys credentials:
	EnvAccountName = "AZURE_ACCOUNT_NAME"
	EnvAccountKey  = "AZURE_ACCOUNT_KEY"

	// Client secret credentials:
	EnvTenantID = "AZURE_TENANT_ID"
	// nolint: gosec // Potential hardcoded credentials
	EnvClientID = "AZURE_CLIENT_ID"
	// nolint: gosec // Potential hardcoded credentials
	EnvSecret = "AZURE_CLIENT_SECRET"

	// When testing default azure credentials, we use CLI credentials which in
	// turn authenticates using a certificate

	// Common parameters:
	EnvContainer = "AZURE_CONTAINER"
	EnvRealm     = "AZURE_REALM"

	// Enables debug logging for Azure SDK.
	EnvDebugLog       = "AZURE_DEBUGLOG"
	EnvDebugLogEvents = "AZURE_DEBUGLOG_EVENTS"

	// specifies driver to use, one of: "azure", "azure_v2"
	EnvDriverVersion = "AZURE_DRIVER_VERSION"

	// Retry configuration:
	EnvMaxRetries      = "AZURE_MAX_RETRIES"
	EnvRetryTryTimeout = "AZURE_RETRY_TRY_TIMEOUT"
	EnvRetryDelay      = "AZURE_RETRY_DELAY"
	EnvMaxRetryDelay   = "AZURE_MAX_RETRY_DELAY"
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

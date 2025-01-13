package v2

import (
	"fmt"
	"strings"
	"time"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
)

type driverParameters struct {
	// One of `shared_key,client_secret,default_credentials`
	credentialsType string

	// client_secret:
	tenantID string
	clientID string
	secret   string

	// shared key:
	accountName string
	accountKey  string

	container            string
	realm                string
	serviceURL           string
	root                 string
	trimLegacyRootPrefix bool

	poolInitialInterval time.Duration
	poolMaxInterval     time.Duration
	poolMaxElapsedTime  time.Duration

	debugLog       bool
	debugLogEvents []azlog.Event
}

// ParseParameters parses parameters for v2 driver version, and returns object
// that is used/should be used only by this package, hence it being private
func ParseParameters(parameters map[string]any) (any, error) {
	var err error
	res := new(driverParameters)

	credsTypeRaw, ok := parameters[common.ParamCredentialsType]
	if !ok {
		// in case when credentials type was not specified, we default to
		// shared key creds to keep backwards-compatibility
		res.credentialsType = common.CredentialsTypeSharedKey
	} else {
		res.credentialsType = fmt.Sprint(credsTypeRaw)
	}

	switch res.credentialsType {
	case common.CredentialsTypeSharedKey, "":
		tmp, ok := parameters[common.ParamAccountName]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamAccountName)
		}
		res.accountName = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamAccountKey]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamAccountKey)
		}
		res.accountKey = fmt.Sprint(tmp)
	case common.CredentialsTypeClientSecret:
		tmp, ok := parameters[common.ParamClientID]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamClientID)
		}
		res.clientID = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamTenantID]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamTenantID)
		}
		res.tenantID = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamSecret]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamSecret)
		}
		res.secret = fmt.Sprint(tmp)
	case common.CredentialsTypeDefaultCredentials:
		// Azure SDK will try to auto-detect instnance-credentials, env vars,
		// etc... So there are no extra config variables here.
	default:
		return nil, fmt.Errorf("credentials type %q is invalid", res.credentialsType)
	}

	_, ok = parameters[common.ParamDebugLog]
	if ok {
		res.debugLog, err = parse.Bool(parameters, common.ParamDebugLog, false)
		if err != nil {
			return nil, fmt.Errorf("parsing parameter %s: %w", common.ParamDebugLog, err)
		}
	}
	res.debugLogEvents = make([]azlog.Event, 0)
	if res.debugLog {
		debugLogEventsRaw, ok := parameters[common.ParamDebugLogEvents]
		if ok {
			for _, debugLogEventRaw := range strings.Split(debugLogEventsRaw.(string), ",") {
				switch strings.ToLower(debugLogEventRaw) {
				case strings.ToLower(string(azlog.EventRequest)):
					res.debugLogEvents = append(res.debugLogEvents, azlog.EventRequest)
				case strings.ToLower(string(azlog.EventResponse)):
					res.debugLogEvents = append(res.debugLogEvents, azlog.EventResponse)
				case strings.ToLower(string(azlog.EventResponseError)):
					res.debugLogEvents = append(res.debugLogEvents, azlog.EventResponseError)
				case strings.ToLower(string(azlog.EventRetryPolicy)):
					res.debugLogEvents = append(res.debugLogEvents, azlog.EventRetryPolicy)
				case strings.ToLower(string(azlog.EventLRO)):
					res.debugLogEvents = append(res.debugLogEvents, azlog.EventLRO)
				default:
					return nil, fmt.Errorf("parsing parameter %s: unrecognized event %q", common.ParamDebugLogEvents, debugLogEventRaw)
				}
			}
		}
		// else {
		// NOTE(prozlach): empty res.debugLogEvents slice  means do not
		// filter events, just print everything
		// }
	}

	container, ok := parameters[common.ParamContainer]
	if !ok || fmt.Sprint(container) == "" {
		return nil, fmt.Errorf("no %s parameter provided", common.ParamContainer)
	}
	res.container = fmt.Sprint(container)

	realm, ok := parameters[common.ParamRealm]
	if !ok || fmt.Sprint(realm) == "" {
		res.realm = "core.windowns.net"
	} else {
		res.realm = fmt.Sprint(realm)
	}

	serviceURL, ok := parameters[common.ParamServiceURLKey]
	if !ok || serviceURL == "" {
		accountName, ok := parameters[common.ParamAccountName]
		if !ok || fmt.Sprint(accountName) == "" {
			return nil, fmt.Errorf(
				"unable to derive service URL as %q was not specified, "+
					"provide %q or %q",
				common.ParamServiceURLKey,
				common.ParamServiceURLKey, common.ParamAccountName,
			)
		}
		res.serviceURL = fmt.Sprintf("https://%s.blob.%s", accountName, res.realm)
	} else {
		res.serviceURL = fmt.Sprint(serviceURL)
	}

	root, ok := parameters[common.ParamRootDirectory]
	if !ok {
		res.root = ""
	} else {
		rootTrimmed := strings.Trim(fmt.Sprint(root), "/")
		if rootTrimmed != "" {
			rootTrimmed += "/"
		}
		res.root = rootTrimmed
	}

	poolInitialInterval, ok := parameters[common.ParamPoolInitialInterval]
	if !ok {
		res.poolInitialInterval = DefaultPoolInitialInterval
	} else {
		tmp, err := time.ParseDuration(fmt.Sprint(poolInitialInterval))
		if err != nil {
			return nil, fmt.Errorf("unable to parse parameter %q = %q: %w", common.ParamPoolInitialInterval, poolInitialInterval, err)
		}
		res.poolInitialInterval = tmp
	}

	poolMaxInterval, ok := parameters[common.ParamPoolMaxInterval]
	if !ok {
		res.poolMaxInterval = DefaultPoolMaxInterval
	} else {
		tmp, err := time.ParseDuration(fmt.Sprint(poolMaxInterval))
		if err != nil {
			return nil, fmt.Errorf("unable to parse parameter %q = %q: %w", common.ParamPoolMaxInterval, poolMaxInterval, err)
		}
		res.poolMaxInterval = tmp
	}

	poolMaxElapsedTime, ok := parameters[common.ParamPoolMaxElapsedTime]
	if !ok {
		res.poolMaxElapsedTime = DefaultPoolMaxElapsedTime
	} else {
		tmp, err := time.ParseDuration(fmt.Sprint(poolMaxElapsedTime))
		if err != nil {
			return nil, fmt.Errorf("unable to parse parameter %q = %q: %w", common.ParamPoolMaxElapsedTime, poolMaxElapsedTime, err)
		}
		res.poolMaxElapsedTime = tmp
	}

	useLegacyRootPrefix, err := common.InferRootPrefixConfiguration(parameters)
	if err != nil {
		return nil, err
	}
	res.trimLegacyRootPrefix = !useLegacyRootPrefix

	return res, nil
}

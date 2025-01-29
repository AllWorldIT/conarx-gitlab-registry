package v2

import (
	"fmt"
	"strings"
	"time"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
)

type DriverParameters struct {
	// One of `shared_key,client_secret,default_credentials`
	CredentialsType string

	// client_secret:
	TenantID string
	ClientID string
	Secret   string

	// shared key:
	AccountName string
	AccountKey  string

	Container            string
	Realm                string
	ServiceURL           string
	Root                 string
	TrimLegacyRootPrefix bool

	PoolInitialInterval time.Duration
	PoolMaxInterval     time.Duration
	poolMaxElapsedTime  time.Duration

	DebugLog       bool
	DebugLogEvents []azlog.Event
}

// ParseParameters parses parameters for v2 driver version, and returns object
// that is used/should be used only by this package, hence it being private
func ParseParameters(parameters map[string]any) (any, error) {
	var err error
	res := new(DriverParameters)

	credsTypeRaw, ok := parameters[common.ParamCredentialsType]
	if !ok {
		// in case when credentials type was not specified, we default to
		// shared key creds to keep backwards-compatibility
		res.CredentialsType = common.CredentialsTypeSharedKey
	} else {
		res.CredentialsType = fmt.Sprint(credsTypeRaw)
	}

	switch res.CredentialsType {
	case common.CredentialsTypeSharedKey, "":
		tmp, ok := parameters[common.ParamAccountName]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamAccountName)
		}
		res.AccountName = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamAccountKey]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamAccountKey)
		}
		res.AccountKey = fmt.Sprint(tmp)
	case common.CredentialsTypeClientSecret:
		tmp, ok := parameters[common.ParamClientID]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamClientID)
		}
		res.ClientID = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamTenantID]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamTenantID)
		}
		res.TenantID = fmt.Sprint(tmp)

		tmp, ok = parameters[common.ParamSecret]
		if !ok || fmt.Sprint(tmp) == "" {
			return nil, fmt.Errorf("no %s parameter provided", common.ParamSecret)
		}
		res.Secret = fmt.Sprint(tmp)
	case common.CredentialsTypeDefaultCredentials:
		// Azure SDK will try to auto-detect instnance-credentials, env vars,
		// etc... So there are no extra config variables here.
	default:
		return nil, fmt.Errorf("credentials type %q is invalid", res.CredentialsType)
	}

	_, ok = parameters[common.ParamDebugLog]
	if ok {
		res.DebugLog, err = parse.Bool(parameters, common.ParamDebugLog, false)
		if err != nil {
			return nil, fmt.Errorf("parsing parameter %s: %w", common.ParamDebugLog, err)
		}
	}
	res.DebugLogEvents = make([]azlog.Event, 0)
	if res.DebugLog {
		debugLogEventsRaw, ok := parameters[common.ParamDebugLogEvents]
		if ok {
			for _, debugLogEventRaw := range strings.Split(debugLogEventsRaw.(string), ",") {
				switch strings.ToLower(debugLogEventRaw) {
				case strings.ToLower(string(azlog.EventRequest)):
					res.DebugLogEvents = append(res.DebugLogEvents, azlog.EventRequest)
				case strings.ToLower(string(azlog.EventResponse)):
					res.DebugLogEvents = append(res.DebugLogEvents, azlog.EventResponse)
				case strings.ToLower(string(azlog.EventResponseError)):
					res.DebugLogEvents = append(res.DebugLogEvents, azlog.EventResponseError)
				case strings.ToLower(string(azlog.EventRetryPolicy)):
					res.DebugLogEvents = append(res.DebugLogEvents, azlog.EventRetryPolicy)
				case strings.ToLower(string(azlog.EventLRO)):
					res.DebugLogEvents = append(res.DebugLogEvents, azlog.EventLRO)
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
	res.Container = fmt.Sprint(container)

	realm, ok := parameters[common.ParamRealm]
	if !ok || fmt.Sprint(realm) == "" {
		res.Realm = "core.windows.net"
	} else {
		res.Realm = fmt.Sprint(realm)
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
		res.ServiceURL = fmt.Sprintf("https://%s.blob.%s", accountName, res.Realm)
	} else {
		res.ServiceURL = fmt.Sprint(serviceURL)
	}

	root, ok := parameters[common.ParamRootDirectory]
	if !ok {
		res.Root = ""
	} else {
		rootTrimmed := strings.Trim(fmt.Sprint(root), "/")
		if rootTrimmed != "" {
			rootTrimmed += "/"
		}
		res.Root = rootTrimmed
	}

	poolInitialInterval, ok := parameters[common.ParamPoolInitialInterval]
	if !ok {
		res.PoolInitialInterval = DefaultPoolInitialInterval
	} else {
		tmp, err := time.ParseDuration(fmt.Sprint(poolInitialInterval))
		if err != nil {
			return nil, fmt.Errorf("unable to parse parameter %q = %q: %w", common.ParamPoolInitialInterval, poolInitialInterval, err)
		}
		res.PoolInitialInterval = tmp
	}

	poolMaxInterval, ok := parameters[common.ParamPoolMaxInterval]
	if !ok {
		res.PoolMaxInterval = DefaultPoolMaxInterval
	} else {
		tmp, err := time.ParseDuration(fmt.Sprint(poolMaxInterval))
		if err != nil {
			return nil, fmt.Errorf("unable to parse parameter %q = %q: %w", common.ParamPoolMaxInterval, poolMaxInterval, err)
		}
		res.PoolMaxInterval = tmp
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
	res.TrimLegacyRootPrefix = !useLegacyRootPrefix

	return res, nil
}

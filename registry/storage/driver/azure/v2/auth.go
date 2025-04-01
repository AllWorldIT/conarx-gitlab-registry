package v2

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/registry/storage/driver/base"
)

const (
	// NOTE(prozlach): Time period in the past, starting from time.Now(), for which
	// the signed URL is already valid. Meant to minimize issues caused by small time
	// synchronization issues. Value follows the one set by upstream.
	clockSkewTolerance = 10 * time.Second
	UDCGracePeriod     = 30.0 * time.Minute
	UDCExpiryTime      = 48.0 * time.Hour
)

// signer abstracts the specifics of a blob SAS and is specialized
// for the different authentication credentials
type signer interface {
	Sign(context.Context, *sas.BlobSignatureValues) (sas.QueryParameters, error)
}

type urlSigner interface {
	SignBlobURL(context.Context, string, time.Time) (string, error)
}

var _ signer = (*sharedKeySigner)(nil)

type sharedKeySigner struct {
	cred *azblob.SharedKeyCredential
}

func (s *sharedKeySigner) Sign(_ context.Context, signatureValues *sas.BlobSignatureValues) (sas.QueryParameters, error) {
	return signatureValues.SignWithSharedKey(s.cred)
}

var _ signer = (*clientTokenSigner)(nil)

type clientTokenSigner struct {
	client *azblob.Client
	cred   azcore.TokenCredential

	udc       *service.UserDelegationCredential
	udcMutex  sync.Mutex
	udcExpiry time.Time
}

func (s *clientTokenSigner) refreshUDC(ctx context.Context) (*service.UserDelegationCredential, error) {
	s.udcMutex.Lock()
	defer s.udcMutex.Unlock()

	now := time.Now().UTC()
	if s.udc == nil || s.udcExpiry.Sub(now) < UDCGracePeriod {
		// reissue user delegation credential
		startTime := now.Add(-10 * time.Second)
		expiryTime := startTime.Add(UDCExpiryTime)
		info := service.KeyInfo{
			Start:  to.Ptr(startTime.UTC().Format(sas.TimeFormat)),
			Expiry: to.Ptr(expiryTime.UTC().Format(sas.TimeFormat)),
		}
		udc, err := s.client.ServiceClient().GetUserDelegationCredential(ctx, info, nil)
		if err != nil {
			return nil, fmt.Errorf("creating user delegation credentials: %w", err)
		}
		s.udc = udc
		s.udcExpiry = expiryTime
	}
	return s.udc, nil
}

func (s *clientTokenSigner) Sign(ctx context.Context, signatureValues *sas.BlobSignatureValues) (sas.QueryParameters, error) {
	udc, err := s.refreshUDC(ctx)
	if err != nil {
		return sas.QueryParameters{}, fmt.Errorf("refreshing UDC credentials: %w", err)
	}
	return signatureValues.SignWithUserDelegation(udc)
}

var _ urlSigner = (*urlSignerImpl)(nil)

type urlSignerImpl struct {
	si signer
}

func (s *urlSignerImpl) SignBlobURL(ctx context.Context, blobURL string, expires time.Time) (string, error) {
	urlParts, err := sas.ParseURL(blobURL)
	if err != nil {
		return "", fmt.Errorf("parsing url %q to be signed: %w", urlParts.String(), err)
	}
	perms := sas.BlobPermissions{Read: true}
	signatureValues := &sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-1 * clockSkewTolerance),
		ExpiryTime:    expires,
		Permissions:   perms.String(),
		ContainerName: urlParts.ContainerName,
		BlobName:      urlParts.BlobName,
	}
	urlParts.SAS, err = s.si.Sign(ctx, signatureValues)
	if err != nil {
		return "", fmt.Errorf("signing URL %q: %w", urlParts.String(), err)
	}
	return urlParts.String(), nil
}

func newSharedKeyCredentialsClient(params *DriverParameters) (*Driver, error) {
	cred, err := azblob.NewSharedKeyCredential(params.AccountName, params.AccountKey)
	if err != nil {
		return nil, fmt.Errorf("creating shared key credentials: %w", err)
	}

	opts := azcore.ClientOptions{
		PerRetryPolicies: []policy.Policy{newRetryNotificationPolicy()},
		Logging: policy.LogOptions{
			AllowedHeaders: []string{
				"x-ms-error-code",
				"Retry-After",
				"Retry-After-Ms",
				"If-Match",
				"x-ms-blob-condition-appendpos",
			},
			AllowedQueryParams: []string{"comp"},
		},
	}
	if params.Transport != nil {
		opts.Transport = &http.Client{
			Transport: params.Transport,
		}
	}
	client, err := azblob.NewClientWithSharedKeyCredential(
		params.ServiceURL,
		cred,
		&azblob.ClientOptions{
			ClientOptions: opts,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client using shared key credentials: %w", err)
	}

	d := &driver{
		client: client.ServiceClient().NewContainerClient(params.Container),
		signer: &urlSignerImpl{
			si: &sharedKeySigner{
				cred: cred,
			},
		},
	}
	commonClientSetup(params, d)

	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

func newTokenClient(params *DriverParameters) (*Driver, error) {
	var cred azcore.TokenCredential
	var err error

	opts := azcore.ClientOptions{
		PerRetryPolicies: []policy.Policy{newRetryNotificationPolicy()},
		Logging: policy.LogOptions{
			AllowedHeaders: []string{
				"x-ms-error-code",
				"Retry-After",
				"Retry-After-Ms",
				"If-Match",
				"x-ms-blob-condition-appendpos",
			},
			AllowedQueryParams: []string{"comp"},
		},
	}
	if params.Transport != nil {
		opts.Transport = &http.Client{
			Transport: params.Transport,
		}
	}
	if params.CredentialsType == common.CredentialsTypeClientSecret {
		cred, err = azidentity.NewClientSecretCredential(
			params.TenantID, params.ClientID, params.Secret,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("creating new client-secret credential: %w", err)
		}
	} else {
		// params.credentialsType == credentialsTypeDefaultCredentials
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("creating default azure credentials: %w", err)
		}
	}

	client, err := azblob.NewClient(
		params.ServiceURL,
		cred,
		&azblob.ClientOptions{
			ClientOptions: opts,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating azure client: %w", err)
	}

	d := &driver{
		client: client.ServiceClient().NewContainerClient(params.Container),
		signer: &urlSignerImpl{
			si: &clientTokenSigner{
				cred:   cred,
				client: client,
			},
		},
	}
	commonClientSetup(params, d)

	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

func commonClientSetup(params *DriverParameters, d *driver) {
	d.Pather = common.NewPather(params.Root, !params.TrimLegacyRootPrefix)

	d.poolInitialInterval = params.PoolInitialInterval
	d.poolMaxInterval = params.PoolMaxInterval
	d.poolMaxElapsedTime = params.PoolMaxElapsedTime

	d.maxRetries = params.MaxRetries
	d.retryTryTimeout = params.RetryTryTimeout
	d.retryDelay = params.RetryDelay
	d.maxRetryDelay = params.MaxRetryDelay

	if params.DebugLog {
		if len(params.DebugLogEvents) > 0 {
			azlog.SetEvents(params.DebugLogEvents...)
		}
		logger := params.Logger
		azlog.SetListener(func(cls azlog.Event, msg string) {
			logger.WithField("event_type", cls).Debug(msg)
		})
	}
}

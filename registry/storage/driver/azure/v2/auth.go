package v2

import (
	"context"
	"fmt"
	"time"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/sirupsen/logrus"
)

// NOTE(prozlach): Time period in the past, starting from time.Now(), for which
// the signed URL is already valid. Meant to minimize issues caused by small time
// synchronization issues. Value follows the one set by upstream.
const clockSkewTolerance = 10 * time.Second

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

func newSharedKeyCredentialsClient(params *driverParameters) (*Driver, error) {
	cred, err := azblob.NewSharedKeyCredential(params.accountName, params.accountKey)
	if err != nil {
		return nil, fmt.Errorf("creating shared key credentials: %w", err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(params.serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating client using shared key credentials: %w", err)
	}

	d := &driver{
		client: client.ServiceClient().NewContainerClient(params.container),
		signer: &urlSignerImpl{
			si: &sharedKeySigner{
				cred: cred,
			},
		},
	}
	commonClientSetup(params, d)

	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

func commonClientSetup(params *driverParameters, d *driver) {
	d.Pather = common.NewPather(params.root, !params.trimLegacyRootPrefix)

	d.poolInitialInterval = params.poolInitialInterval
	d.poolMaxInterval = params.poolMaxInterval
	d.poolMaxElapsedTime = params.poolMaxElapsedTime

	if params.debugLog {
		if len(params.debugLogEvents) > 0 {
			azlog.SetEvents(params.debugLogEvents...)
		}
		logrus.SetFormatter(&logrus.TextFormatter{
			DisableQuote: true,
		})
		logrus.SetLevel(logrus.DebugLevel)
		azlog.SetListener(func(cls azlog.Event, msg string) {
			logrus.WithField("event_type", cls).Debug(msg)
		})
	}
}

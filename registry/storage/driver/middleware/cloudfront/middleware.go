// Package middleware - cloudfront wrapper for storage libs
// N.B. currently only works with S3, not arbitrary sites
package middleware

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/cloudfront/sign"
	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
)

// cloudFrontStorageMiddleware provides a simple implementation of layerHandler that
// constructs temporary signed CloudFront URLs from the storagedriver layer URL,
// then issues HTTP Temporary Redirects to this CloudFront content URL.
type cloudFrontStorageMiddleware struct {
	storagedriver.StorageDriver
	awsIPs    *awsIPs
	urlSigner *sign.URLSigner
	baseURL   string
	duration  time.Duration
}

var _ storagedriver.StorageDriver = &cloudFrontStorageMiddleware{}

// newCloudFrontLayerHandler constructs and returns a new CloudFront
// LayerHandler implementation.
// Required options: baseurl, privatekey, keypairid

// Optional options: ipFilteredBy, awsregion
// ipfilteredby: valid value "none|aws|awsregion". "none", do not filter any IP, default value. "aws", only aws IP goes
//
//	to S3 directly. "awsregion", only regions listed in awsregion options goes to S3 directly
//
// awsregion: a comma separated string of AWS regions.
func newCloudFrontStorageMiddleware(storageDriver storagedriver.StorageDriver, options map[string]any) (storagedriver.StorageDriver, error) {
	// parse baseurl
	base, ok := options["baseurl"]
	if !ok {
		return nil, fmt.Errorf("no baseurl provided")
	}
	baseURL, ok := base.(string)
	if !ok {
		return nil, fmt.Errorf("baseurl must be a string")
	}
	if !strings.Contains(baseURL, "://") {
		baseURL = "https://" + baseURL
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("invalid baseurl: %v", err)
	}

	// parse privatekey to get pkPath
	pk, ok := options["privatekey"]
	if !ok {
		return nil, fmt.Errorf("no privatekey provided")
	}
	pkPath, ok := pk.(string)
	if !ok {
		return nil, fmt.Errorf("privatekey must be a string")
	}

	// parse keypairid
	kpid, ok := options["keypairid"]
	if !ok {
		return nil, fmt.Errorf("no keypairid provided")
	}
	keypairID, ok := kpid.(string)
	if !ok {
		return nil, fmt.Errorf("keypairid must be a string")
	}

	// get urlSigner from the file specified in pkPath
	// nolint: gosec
	pkBytes, err := os.ReadFile(pkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read privatekey file: %s", err)
	}

	block, _ := pem.Decode(pkBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode private key as an rsa private key")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	urlSigner := sign.NewURLSigner(keypairID, privateKey)

	// parse duration
	duration := 20 * time.Minute
	if d, ok := options["duration"]; ok {
		switch d := d.(type) {
		case time.Duration:
			duration = d
		case string:
			dur, err := time.ParseDuration(d)
			if err != nil {
				return nil, fmt.Errorf("invalid duration: %s", err)
			}
			duration = dur
		}
	}

	// parse updatefrequency
	updateFrequency := defaultUpdateFrequency
	// #2447 introduced a typo. Support it for backward compatibility.
	if _, ok := options["updatefrenquency"]; ok {
		options["updatefrequency"] = options["updatefrenquency"]
		dcontext.GetLogger(context.Background()).Warn("cloudfront updatefrenquency is deprecated. Please use updatefrequency")
	}
	if u, ok := options["updatefrequency"]; ok {
		switch u := u.(type) {
		case time.Duration:
			updateFrequency = u
		case string:
			updateFreq, err := time.ParseDuration(u)
			if err != nil {
				return nil, fmt.Errorf("invalid updatefrequency: %s", err)
			}
			duration = updateFreq
		}
	}

	// parse iprangesurl
	ipRangesURL := defaultIPRangesURL
	if i, ok := options["iprangesurl"]; ok {
		iprangeurl, ok := i.(string)
		if !ok {
			return nil, fmt.Errorf("iprangesurl must be a string")
		}
		ipRangesURL = iprangeurl
	}

	// parse ipfilteredby
	var awsIPs *awsIPs
	if i, ok := options["ipfilteredby"]; ok {
		ipFilteredBy, ok := i.(string)
		if !ok {
			return nil, fmt.Errorf("ipfilteredby only allows a string with the following value: none|aws|awsregion")
		}

		switch strings.ToLower(strings.TrimSpace(ipFilteredBy)) {
		case "", "none":
			awsIPs = nil
		case "aws":
			awsIPs = newAWSIPs(ipRangesURL, updateFrequency, nil)
		case "awsregion":
			var awsRegion []string
			i, ok := options["awsregion"]
			if !ok {
				return nil, fmt.Errorf("awsRegion is not defined")
			}
			regions, ok := i.(string)
			if !ok {
				return nil, fmt.Errorf("awsRegion must be a comma separated string of valid aws regions")
			}
			for _, awsRegions := range strings.Split(regions, ",") {
				awsRegion = append(awsRegion, strings.ToLower(strings.TrimSpace(awsRegions)))
			}
			awsIPs = newAWSIPs(ipRangesURL, updateFrequency, awsRegion)
		default:
			return nil, fmt.Errorf("ipfilteredby only allows a string the following value: none|aws|awsregion")
		}
	}

	return &cloudFrontStorageMiddleware{
		StorageDriver: storageDriver,
		urlSigner:     urlSigner,
		baseURL:       baseURL,
		duration:      duration,
		awsIPs:        awsIPs,
	}, nil
}

// URLFor attempts to find a url which may be used to retrieve the file at the given path.
// Returns an error if the file cannot be found.
func (lh *cloudFrontStorageMiddleware) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	keyer, ok := lh.StorageDriver.(common.S3BucketKeyer)
	if !ok {
		dcontext.GetLogger(ctx).Warn("the CloudFront middleware does not support this backend storage driver")
		return lh.StorageDriver.URLFor(ctx, path, options)
	}

	if eligibleForS3(ctx, lh.awsIPs) {
		return lh.StorageDriver.URLFor(ctx, path, options)
	}

	// Get signed cloudfront url.
	cfURL, err := lh.urlSigner.Sign(lh.baseURL+keyer.S3BucketKey(path), time.Now().Add(lh.duration))
	if err != nil {
		return "", err
	}
	return cfURL, nil
}

// init registers the cloudfront layerHandler backend.
func init() {
	// nolint: gosec // ignore when backend is already registered
	_ = storagemiddleware.Register("cloudfront", storagemiddleware.InitFunc(newCloudFrontStorageMiddleware))
}

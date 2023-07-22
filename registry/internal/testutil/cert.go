package testutil

import (
	"crypto/x509"
	"encoding/pem"
	"os"

	"github.com/docker/libtrust"
)

func WriteTempRootCerts() (certFilePath string, privateKey libtrust.PrivateKey, err error) {
	rootKey, err := makeRootKey()
	if err != nil {
		return "", nil, err
	}
	rootCert, err := makeRootCert(rootKey)
	if err != nil {
		return "", nil, err
	}

	tempFile, err := os.CreateTemp("", "rootCertBundle")
	if err != nil {
		return "", nil, err
	}
	defer tempFile.Close()

	if err = pem.Encode(tempFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootCert.Raw,
	}); err != nil {
		os.Remove(tempFile.Name())
		return "", nil, err
	}

	return tempFile.Name(), rootKey, nil
}

func makeRootCert(rootKey libtrust.PrivateKey) (*x509.Certificate, error) {
	cert, err := libtrust.GenerateCACert(rootKey, rootKey)
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func makeRootKey() (libtrust.PrivateKey, error) {
	key, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		return nil, err
	}

	return key, nil
}

package systemops

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"go.uber.org/zap"
)

// TLSCertPaths contains the paths to the generated TLS certificates
type TLSCertPaths struct {
	KeyPath  string // Path to the certificate private key
	CertPath string // Path to the certificate PEM file
	CAPath   string // Path to the CA certificate
}

// generateTLSCertificates initializes mkcert CA and generates certificates for gitlab.devvm and registry.devvm
// Certificates are saved to /etc/gitlab/ssl directory
// Returns the paths to the generated certificate key, certificate PEM, and CA certificate
func generateTLSCertificates(logger *zap.SugaredLogger, svcIP string) (TLSCertPaths, status.PartialStatus) {
	// Check if certificates already exist
	certPaths, err := findCertificateFiles(logger)
	if err == nil {
		logger.Info("TLS certificates already exist, skipping generation")
		return certPaths, status.PartialStatus{
			Succeeded: status.StatusStageOK,
			DetailedMessage: []string{
				fmt.Sprintf("TLS certificates already exist in %s", constants.GitLabSSLDir),
				fmt.Sprintf("Certificate key: %s", certPaths.KeyPath),
				fmt.Sprintf("Certificate PEM: %s", certPaths.CertPath),
				fmt.Sprintf("CA certificate: %s", certPaths.CAPath),
			},
		}
	}

	// Initialize mkcert CA if not already done
	if err := initMkcertCA(logger); err != nil {
		return TLSCertPaths{}, status.Failed("failed to initialize mkcert CA: %v", err)
	}

	// Generate certificate for gitlab.devvm
	certDomains := []string{constants.GitLabHost, constants.RegistryHost, svcIP}
	if err := generateMkcertCertificate(logger, certDomains...); err != nil {
		return TLSCertPaths{}, status.Failed("failed to generate certificate: %v", err)
	}

	// Find and validate the generated certificate files
	certPaths, err = findCertificateFiles(logger)
	if err != nil {
		return TLSCertPaths{}, status.Failed("failed to locate certificate files: %v", err)
	}

	logger.Info("TLS certificates generated successfully")
	return certPaths, status.PartialStatus{
		Succeeded: status.StatusStageOK,
		DetailedMessage: []string{
			fmt.Sprintf("TLS certificates generated in %s", constants.GitLabSSLDir),
			fmt.Sprintf("Certificate for domains %v have been created", certDomains),
			fmt.Sprintf("Certificate key: %s", certPaths.KeyPath),
			fmt.Sprintf("Certificate PEM: %s", certPaths.CertPath),
			fmt.Sprintf("CA certificate: %s", certPaths.CAPath),
		},
	}
}

// initMkcertCA initializes the mkcert CA if not already installed
func initMkcertCA(logger *zap.SugaredLogger) error {
	cmd := "mkcert -install"
	if _, err := executeCmdAsDevWithBash(logger, "", cmd, "Initializing mkcert CA"); err != nil {
		return fmt.Errorf("failed to initialize mkcert CA: %w", err)
	}
	return nil
}

// generateMkcertCertificate generates a certificate for the given domain using mkcert
// and saves it to /etc/gitlab/ssl directory
func generateMkcertCertificate(logger *zap.SugaredLogger, domains ...string) error {
	// Ensure /etc/gitlab/ssl directory exists
	if err := os.MkdirAll(constants.GitLabSSLDir, 0o750); err != nil {
		return fmt.Errorf("failed to create %s directory: %w", constants.GitLabSSLDir, err)
	}

	// Generate certificate using mkcert directly in /etc/gitlab/ssl
	// mkcert outputs files as <domain>.pem and <domain>-key.pem
	cmdArgs := append([]string{"mkcert"}, domains...)
	_, exitCode, err := executeCmd(logger, constants.GitLabSSLDir, cmdArgs...)
	if err != nil || exitCode != 0 {
		return fmt.Errorf("mkcert failed for domains %v: %w (exit code: %d)", domains, err, exitCode)
	}

	logger.Infof("Certificate generated for %v in %s", domains, constants.GitLabSSLDir)
	return nil
}

var (
	// Matches: foo-key.pem, bar-key.pem
	keyPemRegex = regexp.MustCompile(`^\S+-key\.pem$`)

	// Matches: foo.pem, bar.pem, but NOT foo-key.pem
	pemRegex = regexp.MustCompile(`^\S+\.pem$`)

	// CA cert pattern: mkcert_development_CA_*.pem in /etc/ssl/certs
	caPattern = regexp.MustCompile(`^mkcert_development_CA_.*\.pem$`)
)

// isKeyPem checks if the filename matches the key PEM pattern
func isKeyPem(filename string) bool {
	return keyPemRegex.MatchString(filename)
}

// isNonKeyPem checks if the filename is a PEM file but not a key file
func isNonKeyPem(filename string) bool {
	return pemRegex.MatchString(filename) && !keyPemRegex.MatchString(filename)
}

// findCertificateFiles locates the mkcert-generated certificate files in GitLabSSLDir
// Returns an error if there are multiple matches or if required files are missing
func findCertificateFiles(logger *zap.SugaredLogger) (TLSCertPaths, error) {
	var keyFiles, certFiles, caFiles []string

	// Scan /etc/gitlab/ssl for key and cert files
	entries, err := os.ReadDir(constants.GitLabSSLDir)
	if err != nil {
		return TLSCertPaths{}, fmt.Errorf("failed to read directory %s: %w", constants.GitLabSSLDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if isKeyPem(name) {
			keyFiles = append(keyFiles, filepath.Join(constants.GitLabSSLDir, name))
		} else if isNonKeyPem(name) {
			certFiles = append(certFiles, filepath.Join(constants.GitLabSSLDir, name))
		}
	}

	// Scan /etc/ssl/certs for CA certificate
	caCertDir := "/etc/ssl/certs"
	caEntries, err := os.ReadDir(caCertDir)
	if err != nil {
		return TLSCertPaths{}, fmt.Errorf("failed to read directory %s: %w", caCertDir, err)
	}

	for _, entry := range caEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if caPattern.MatchString(name) {
			caFiles = append(caFiles, filepath.Join(caCertDir, name))
		}
	}

	// Validate that we have exactly one of each file type
	if len(keyFiles) == 0 {
		return TLSCertPaths{}, fmt.Errorf("no certificate key file found in %s", constants.GitLabSSLDir)
	}
	if len(keyFiles) > 1 {
		return TLSCertPaths{}, fmt.Errorf("multiple certificate key files found in %s: %v", constants.GitLabSSLDir, keyFiles)
	}

	if len(certFiles) == 0 {
		return TLSCertPaths{}, fmt.Errorf("no certificate PEM file found in %s", constants.GitLabSSLDir)
	}
	if len(certFiles) > 1 {
		return TLSCertPaths{}, fmt.Errorf("multiple certificate PEM files found in %s: %v", constants.GitLabSSLDir, certFiles)
	}

	if len(caFiles) == 0 {
		return TLSCertPaths{}, fmt.Errorf("no CA certificate file found in %s", caCertDir)
	}
	if len(caFiles) > 1 {
		return TLSCertPaths{}, fmt.Errorf("multiple CA certificate files found in %s: %v", caCertDir, caFiles)
	}

	paths := TLSCertPaths{
		KeyPath:  keyFiles[0],
		CertPath: certFiles[0],
		CAPath:   caFiles[0],
	}

	logger.Infof("Located certificate files - Key: %s, Cert: %s, CA: %s", paths.KeyPath, paths.CertPath, paths.CAPath)
	return paths, nil
}

package templates

import (
	"bytes"
	"fmt"
	"text/template"
)

// ============================================
// Add these to manifests.go
// ============================================

// GitLabValuesData holds data for GitLab Helm values template
type GitLabValuesData struct {
	// Domain and hosts
	Domain       string
	GitLabHost   string
	RegistryHost string
	ExternalIP   string

	// PostgreSQL configuration
	PostgreSQLHost     string
	PostgreSQLPort     int
	GitLabDBName       string
	GitLabDBUsername   string
	RegistryDBName     string
	RegistryDBUsername string
	CIDBName           string
	CIDBUsername       string

	// Redis/Valkey configuration
	RedisHost     string
	RedisPort     int
	RedisDatabase int
	RedisUsername string

	// Secret names
	GitLabPostgresSecretName   string
	CIPostgresSecretName       string
	RegistryPostgresSecretName string
	RedisSecretName            string
	MinioRegistrySecretName    string
	MinioBackupSecretName      string
	MinioUnifiedSecretName     string
	InitialRootPasswordSecret  string
	LicenseSecretName          string // Empty string if CE, "gitlab-license" if EE
	PreReceiveHookConfigMap    string
	GitLabTLSSecretName        string
	RegistryTLSSecretName      string

	// Enterprise Edition configuration
	EEVersion bool // Whether EE features are enabled

	// Bucket names
	BackupsBucket    string
	BackupsTmpBucket string

	// Custom registry image configuration
	RegistryImageRepository string // Custom registry image repository (e.g., "10.15.17.1:7000/gitlab-container-registry")
	RegistryImageTag        string // Custom registry image tag (e.g., "abc123-ubi")
}

// ============================================
// Add to Template Data Structs section
// ============================================

// TLSSecretData holds data for TLS secret template
type TLSSecretData struct {
	Name        string
	Namespace   string
	Certificate string // base64 encoded certificate PEM
	Key         string // base64 encoded private key PEM
}

// ============================================
// Add to Template Rendering Functions section
// ============================================

// L2AdvertisementManifest is a static manifest for MetalLB L2 advertisement
const L2AdvertisementManifest = `apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: empty
  namespace: metallb-system`

// MetricsServerValues contains Helm values for metrics-server installation
const MetricsServerValues = `args:
  - "--kubelet-insecure-tls"`

// KindClusterConfigTemplate generates a Kind cluster configuration
const KindClusterConfigTemplate = `---
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: {{.ClusterName}}
# configure network so we can install Calico to allow Network Policies.
networking:
  # disable kindnet
  disableDefaultCNI: true
  # Calico's default subnet
  podSubnet: 192.168.0.0/16
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."{{.InsecureRegistryAddress}}"]
    endpoint = ["http://{{.InsecureRegistryAddress}}"]
nodes:
  - role: control-plane
  - role: worker
    labels:
      ingress-ready: "true"
    extraPortMappings:
      - containerPort: 80
        hostPort: 9080
      - containerPort: 443
        hostPort: 9443
      - containerPort: 32495
        hostPort: 9880
  - role: worker
  - role: worker
`

// ============================================
// Template Data Structs
// ============================================

// IPPoolData holds data for MetalLB IP pool template
type IPPoolData struct {
	MinAddress string
	MaxAddress string
}

// NamespaceData holds data for namespace template
type NamespaceData struct {
	Name string
}

// SecretData holds data for secret template
type SecretData struct {
	Name      string
	Namespace string
	Key       string
	Value     string // Should be base64 encoded
}

// MinioRegistrySecretData holds data for MinIO registry secret template
type MinioRegistrySecretData struct {
	Name      string
	Namespace string
	Config    string // base64 encoded storage config YAML
}

// RegistryStorageConfigData holds data for registry storage configuration template
type RegistryStorageConfigData struct {
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string
	RegionEndpoint string
	RootDirectory  string
}

// MinioUnifiedSecretData holds data for MinIO unified secret template
type MinioUnifiedSecretData struct {
	Name       string
	Namespace  string
	Connection string // base64 encoded connection config YAML
}

// UnifiedConnectionConfigData holds data for unified connection configuration template
type UnifiedConnectionConfigData struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Host            string
	Endpoint        string
}

// S3CmdConfigData holds data for s3cmd configuration template
type S3CmdConfigData struct {
	AccessKey       string
	SecretKey       string
	BucketLocation  string
	HostBase        string
	WebsiteEndpoint string
}

// MinioBackupSecretData holds data for MinIO backup secret template
type MinioBackupSecretData struct {
	Name      string
	Namespace string
	Config    string // base64 encoded s3cmd config
}

// ConfigMapData holds data for configmap template
type ConfigMapData struct {
	Name      string
	Namespace string
	DataKey   string
	DataValue string
}

// KindClusterData holds data for Kind cluster config template
type KindClusterData struct {
	ClusterName             string
	InsecureRegistryAddress string
}

// ============================================
// Template Rendering Functions
// ============================================

// RenderTemplate renders a template with the given data
func RenderTemplate(tmplStr string, data any) (string, error) {
	tmpl, err := template.New("manifest").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// RenderKindClusterConfig renders a Kind cluster configuration
func RenderKindClusterConfig(clusterName, insecureRegistryAddress string) (string, error) {
	return RenderTemplate(KindClusterConfigTemplate, KindClusterData{
		ClusterName:             clusterName,
		InsecureRegistryAddress: insecureRegistryAddress,
	})
}

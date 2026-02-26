package constants

const (
	GCEProject = "dev-package-container-96a3ff34"

	// ============================================
	// File Paths - Booter Status & Cache
	// ============================================
	DefaultLabelsCacheFilePath = "/home/dev/.booter-vmlabels_cache.json"
	DefaultNetworkInfoFilePath = "/home/dev/.booter-network_info.json"
	StatusFilePath             = "/home/dev/.booter-status.json"

	// ============================================
	// File Paths - Service Credentials & State
	// ============================================
	MinioConfPath           = "/etc/default/minio"
	MinioRegistryAccessPath = "/home/dev/.minio-registry-access-info.json"
	MinioBackupAccessPath   = "/home/dev/.minio-backup-access-info.json"
	MinioUnifiedAccessPath  = "/home/dev/.minio-unified-access-info.json"
	PostgreSQLAccessPath    = "/home/dev/.postgresql-access-info.json"
	WireguardUIConfPath     = "/etc/default/wg-easy"
	WireguardUIStatePath    = "/etc/wg-easy/"
	EELicensePath           = "/home/dev/.gitlab-license"
	GitLabSSLDir            = "/etc/gitlab/ssl"
	GitLabHelmValuesPath    = "/home/dev/helm-values.yml"

	// ============================================
	// Kubernetes / Kind Configuration
	// ============================================
	// https://github.com/kubernetes-sigs/kind/releases
	// NOTE(prozlach): It is intentional that we do bind the node image sha,
	// but at the same time we do not specify kind version which will
	// eventually lead to breakages as old node versions are decommissioned in
	// newer versions of kind CLI. This is intended to force periodical update
	// of k8s version while keeping long periods of reproducible builds
	KindClusterNodeImage = "kindest/node:v1.33.4@sha256:25a6018e48dfcaee478f4a59af81157a437f15e6e140bf103f85a2e7cd0cbbf2"
	KindClusterName      = "sandbox"
	KindKubeConfigPath   = "/home/dev/kubeconfig"
	KindCalicoVersion    = "v3.31.0"

	// ============================================
	// Docker / Network Configuration
	// ============================================
	DockerBridgeName     = "br-wg"
	DockerNetworkName    = "wghub"
	ControlPlaneNodeName = KindClusterName + "-control-plane"
	DockerRegistryPort   = 7000
	CNGDirectory         = "/home/dev/CNG/"

	// ============================================
	// User Configuration
	// ============================================
	DevvmUser = "dev"

	// ============================================
	// Service Names
	// ============================================
	WireguardUIServiceName     = "wg-easy"
	MinioServiceName           = "minio"
	PostgreSQLServiceName      = "postgresql"
	ValkeyServiceName          = "valkey-server"
	SystemdNetworkdServiceName = "systemd-networkd"
	DockerServiceName          = "docker"

	// ============================================
	// Minio Configuration
	// ============================================
	// NOTE: MinioEndpoint is dynamically constructed using svcIP:9000
	// to match the pattern used by other services like Valkey
	MinioAliasName           = "devvm"
	MinioUserPlaceholder     = "MINIO_USER_PLACEHOLDER"
	MinioPasswordPlaceholder = "MINIO_PASS_PLACEHOLDER"

	// Minio bucket names - Registry
	MinioRegistryBucketName = "gitlab-registry"

	// Minio bucket names - Backup
	MinioBackupBucketName    = "gitlab-backup"
	MinioBackupTmpBucketName = "gitlab-backup-tmp"

	// Minio bucket names - Unified (GitLab Rails services)
	MinioArtifactsBucketName       = "gitlab-artifacts"
	MinioLfsBucketName             = "git-lfs"
	MinioUploadsBucketName         = "gitlab-uploads"
	MinioPackagesBucketName        = "gitlab-packages"
	MinioMrDiffsBucketName         = "gitlab-mr-diffs"
	MinioTerraformStateBucketName  = "gitlab-terraform-state"
	MinioCiSecureFilesBucketName   = "gitlab-ci-secure-files"
	MinioDependencyProxyBucketName = "gitlab-dependency-proxy"
	MinioBackupsBucketName         = "gitlab-backups"

	// Minio policy names
	MinioRegistryPolicyName = "registry-bucket-policy"
	MinioBackupPolicyName   = "backup-buckets-policy"
	MinioUnifiedPolicyName  = "unified-buckets-policy"

	// Minio bucket location for s3cmd config
	MinioBucketLocation = "us-east-1"

	// ============================================
	// PostgreSQL Configuration
	// ============================================
	PostgreSQLDefaultUser = "postgres"

	// Database names (pre-created)
	GitLabDBName   = "gitlab_db"
	RegistryDBName = "registry_db"
	CIDBName       = "ci_db"

	// Database usernames
	GitLabUsername   = "gitlab_user"
	RegistryUsername = "registry_user"
	CIUsername       = "ci_user"

	// ============================================
	// Valkey Configuration
	// ============================================
	ValkeyHost = "localhost"
	ValkeyPort = 6379
	ValkeyCLI  = "valkey-cli"

	// Valkey usernames
	ValkeyDefaultUsername  = "default" // rails does not support passing username
	ValkeyGitLabUsername   = "gitlab_user"
	ValkeyRegistryUsername = "registry_user"

	// Valkey database assignments
	ValkeyGitLabDB   = 0
	ValkeyRegistryDB = 1

	// ============================================
	// Wireguard UI Placeholders
	// ============================================
	WireguardUIUsernamePlaceholder = "USERNAME_PLACEHOLDER"
	WireguardUIPasswordPlaceholder = "PASSWORD_PLACEHOLDER"
	WireguardUIPublicIPPlaceholder = "PUBLIC_IP_PLACEHOLDER"

	// ============================================
	// Kubernetes Field Manager & Namespaces
	// ============================================
	FieldManagerIDString = "booter"
	GitLabNamespace      = "gitlab"
	MetallbNamespace     = "metallb-system"
	KubeSystemNamespace  = "kube-system"

	// ============================================
	// Helm Repository Configuration
	// ============================================
	MetricsServerRepoName = "metrics-server"
	MetricsServerRepoURL  = "https://kubernetes-sigs.github.io/metrics-server/"
	MetricsServerChart    = "metrics-server"
	MetricsServerRelease  = "metrics-server"
	MetricsServerVersion  = "^3.12"

	GitLabRepoName = "gitlab"
	GitLabRepoURL  = "https://charts.gitlab.io"

	// ============================================
	// Status Stage Names
	// ============================================
	VMlabelsStatusName      = "Reading VM metadata labels"
	NetworkInfoStageName    = "Reading Network addresation information"
	WireguardVPNStageName   = "Wireguard VPN"
	MinioStageName          = "Minio Object Storage"
	ValkeyStageName         = "Valkey Cache"
	PostgresqlStageName     = "PostgreSQL Databases"
	TLSCertsStageName       = "TLS Certificates Generation"
	K8sClusterStageName     = "Ensure Kind cluster is running"
	AuxManifestsStageName   = "Ensure auxiliary k8s manifests are deployed"
	CNGImagesBuildStageName = "Build CNG container registry images"

	// ============================================
	// Timeouts & Intervals
	// ============================================
	DefaultServiceReadyTimeout  = 60
	DefaultServiceReadyInterval = 2
	DefaultK8sOperationTimeout  = 5
	ShutdownTimeout             = 60

	// ============================================
	// DevVM Domain Configuration (add new section)
	// ============================================
	DevvmDomain     = "devvm"
	GitLabHost      = "gitlab." + DevvmDomain
	RegistryHost    = "registry." + GitLabHost
	GitLabRootEmail = "admin@" + GitLabHost

	// ============================================
	// PostgreSQL Configuration (add to existing PostgreSQL section)
	// ============================================
	PostgreSQLPort = 5432

	// ============================================
	// Helm Repository Configuration (add to existing section)
	// ============================================
	GitLabChart   = "gitlab"
	GitLabRelease = "gitlab"

	// ============================================
	// Status Stage Names (add to existing section)
	// ============================================
	GitLabHelmStageName           = "Deploy GitLab Helm chart"
	OmnibusPrerequisitesStageName = "Ensure omnibus prerequisites"
	GitlabOmnibusStageName        = "Install Gitlab Omnibus package"
	OmnibusVipInterface           = "Ensure omnibus Vip dummy interface"
)

package ocischema

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// SchemaVersion provides a pre-initialized version structure for this
// packages version of the manifest.
var SchemaVersion = manifest.Versioned{
	SchemaVersion: 2, // historical value here.. does not pertain to OCI or docker version
	MediaType:     v1.MediaTypeImageManifest,
}

func init() {
	ocischemaFunc := func(b []byte) (distribution.Manifest, distribution.Descriptor, error) {
		m := new(DeserializedManifest)
		err := m.UnmarshalJSON(b)
		if err != nil {
			return nil, distribution.Descriptor{}, err
		}

		dgst := digest.FromBytes(b)
		return m, distribution.Descriptor{Digest: dgst, Size: int64(len(b)), MediaType: v1.MediaTypeImageManifest}, err
	}
	err := distribution.RegisterManifestSchema(v1.MediaTypeImageManifest, ocischemaFunc)
	if err != nil {
		panic(fmt.Sprintf("Unable to register manifest: %s", err))
	}
}

// Manifest defines a ocischema manifest.
type Manifest struct {
	manifest.Versioned

	// This OPTIONAL property contains the type of an artifact when the
	// manifest is used for an artifact. This MUST be set when
	// config.mediaType is set to the empty value.
	ArtifactType string `json:"artifactType,omitempty"`

	// Config references the image configuration as a blob.
	Config distribution.Descriptor `json:"config"`

	// Layers lists descriptors for the layers referenced by the
	// configuration.
	Layers []distribution.Descriptor `json:"layers"`

	// This OPTIONAL property specifies a descriptor of another manifest.
	// This value, used by the referrers API, indicates a relationship to
	// the specified manifest.
	Subject *distribution.Descriptor `json:"subject,omitempty"`

	// Annotations contains arbitrary metadata for the image manifest.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// References returns the descriptors of this manifests references.
func (m Manifest) References() []distribution.Descriptor {
	references := make([]distribution.Descriptor, 0, 2+len(m.Layers))
	references = append(references, m.Config)
	references = append(references, m.Layers...)
	if m.Subject != nil {
		references = append(references, *m.Subject)
	}
	return references
}

// Target returns the target of this manifest.
func (m Manifest) Target() distribution.Descriptor {
	return m.Config
}

// DeserializedManifest wraps Manifest with a copy of the original JSON.
// It satisfies the distribution.Manifest interface.
type DeserializedManifest struct {
	Manifest

	// canonical is the canonical byte representation of the Manifest.
	canonical []byte
}

// FromStruct takes a Manifest structure, marshals it to JSON, and returns a
// DeserializedManifest which contains the manifest and its JSON representation.
func FromStruct(m Manifest) (*DeserializedManifest, error) {
	var deserialized DeserializedManifest
	deserialized.Manifest = m

	var err error
	deserialized.canonical, err = json.MarshalIndent(&m, "", "   ")
	return &deserialized, err
}

// UnmarshalJSON populates a new Manifest struct from JSON data.
func (m *DeserializedManifest) UnmarshalJSON(b []byte) error {
	m.canonical = make([]byte, len(b))
	// store manifest in canonical
	copy(m.canonical, b)

	// Unmarshal canonical JSON into Manifest object
	var testManifest Manifest
	if err := json.Unmarshal(m.canonical, &testManifest); err != nil {
		return err
	}

	if testManifest.MediaType != "" && testManifest.MediaType != v1.MediaTypeImageManifest {
		return fmt.Errorf("if present, mediaType in manifest should be '%s' not '%s'",
			v1.MediaTypeImageManifest, testManifest.MediaType)
	}

	m.Manifest = testManifest

	return nil
}

// MarshalJSON returns the contents of canonical. If canonical is empty,
// marshals the inner contents.
func (m *DeserializedManifest) MarshalJSON() ([]byte, error) {
	if len(m.canonical) > 0 {
		return m.canonical, nil
	}

	return nil, errors.New("JSON representation not initialized in DeserializedManifest")
}

// Payload returns the raw content of the manifest. The contents can be used to
// calculate the content identifier.
func (m DeserializedManifest) Payload() (string, []byte, error) {
	return v1.MediaTypeImageManifest, m.canonical, nil
}

var _ distribution.ManifestV2 = &DeserializedManifest{}

func (m *DeserializedManifest) Version() manifest.Versioned {
	// Media type can be either Docker (`application/vnd.docker.distribution.manifest.v2+json`) or OCI (empty).
	// We need to make it explicit if empty, otherwise we're not able to distinguish between media types.
	if m.Versioned.MediaType == "" {
		m.Versioned.MediaType = v1.MediaTypeImageManifest
	}

	return m.Versioned
}

func (m *DeserializedManifest) ArtifactType() string              { return m.Manifest.ArtifactType }
func (m *DeserializedManifest) Config() distribution.Descriptor   { return m.Target() }
func (m *DeserializedManifest) Layers() []distribution.Descriptor { return m.Manifest.Layers }
func (m *DeserializedManifest) Subject() distribution.Descriptor {
	if m.Manifest.Subject == nil {
		return distribution.Descriptor{}
	}
	return *m.Manifest.Subject
}

func (m *DeserializedManifest) DistributableLayers() []distribution.Descriptor {
	var ll []distribution.Descriptor
	for _, l := range m.Layers() {
		switch l.MediaType {
		case v1.MediaTypeImageLayerNonDistributable, v1.MediaTypeImageLayerNonDistributableGzip:
			continue
		}
		ll = append(ll, l)
	}
	return ll
}

func (m *DeserializedManifest) TotalSize() int64 {
	var layersSize int64
	for _, layer := range m.Layers() {
		layersSize += layer.Size
	}

	return layersSize + m.Config().Size + int64(len(m.canonical))
}

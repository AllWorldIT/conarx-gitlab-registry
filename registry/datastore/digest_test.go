package datastore_test

import (
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestNewDigest(t *testing.T) {
	testCases := []struct {
		name    string
		have    digest.Digest
		want    datastore.Digest
		wantErr bool
	}{
		{
			name: "sha256",
			have: "sha256:bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155",
			want: "01bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155",
		},
		{
			name: "sha512",
			have: "sha512:e7247091e1ff34234e5209de8e69bee4f740b6e303b314d8d46b74120f319d3b78e91f74dc161d8fd84aa136f2236603bef44696b7858ba17740a3f2cf59fe72",
			want: "02e7247091e1ff34234e5209de8e69bee4f740b6e303b314d8d46b74120f319d3b78e91f74dc161d8fd84aa136f2236603bef44696b7858ba17740a3f2cf59fe72",
		},
		{
			name:    "unknown",
			have:    "sha384:7f8376410e97a1357a4060c0ae6e2891174443aeb2d7bc1177959fdb8a32447160c7972eb5c4c7ddf4a1c007130bc95b",
			wantErr: true,
		},
		{
			name:    "zero value",
			have:    "",
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			got, err := datastore.NewDigest(tc.have)

			if tc.wantErr {
				require.Zero(tt, got)
				require.Error(tt, err)
				return
			}
			require.NoError(tt, err)
			require.Equal(tt, tc.want, got)
		})
	}
}

func TestDigest_Parse(t *testing.T) {
	testCases := []struct {
		name       string
		have       datastore.Digest
		want       digest.Digest
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "sha256",
			have: "01bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155",
			want: "sha256:bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155",
		},
		{
			name: "sha512",
			have: "02e7247091e1ff34234e5209de8e69bee4f740b6e303b314d8d46b74120f319d3b78e91f74dc161d8fd84aa136f2236603bef44696b7858ba17740a3f2cf59fe72",
			want: "sha512:e7247091e1ff34234e5209de8e69bee4f740b6e303b314d8d46b74120f319d3b78e91f74dc161d8fd84aa136f2236603bef44696b7858ba17740a3f2cf59fe72",
		},
		{
			name:       "unknown",
			have:       "007f8376410e97a1357a4060c0ae6e2891174443aeb2d7bc1177959fdb8a32447160c7972eb5c4c7ddf4a1c007130bc95b",
			wantErr:    true,
			wantErrMsg: `unknown algorithm prefix "00"`,
		},
		{
			name:       "zero value",
			have:       "",
			wantErr:    true,
			wantErrMsg: "empty digest",
		},
		{
			name:       "invalid algorithm prefix",
			have:       "1",
			wantErr:    true,
			wantErrMsg: "invalid digest length",
		},
		{
			name:       "sha256 with no algorithm prefix",
			have:       "01e85a20d32f249c323ed4085026b6b0ee264788276aa7c06cf4b5da1669067a",
			wantErr:    true,
			wantErrMsg: "invalid checksum digest length",
		},
		{
			name:       "sha512 with no algorithm prefix",
			have:       "02247091e1ff34234e5209de8e69bee4f740b6e303b314d8d46b74120f319d3b78e91f74dc161d8fd84aa136f2236603bef44696b7858ba17740a3f2cf59fe72",
			wantErr:    true,
			wantErrMsg: "invalid checksum digest length",
		},
		{
			name:       "no checksum",
			have:       "01",
			wantErr:    true,
			wantErrMsg: "no checksum",
		},
		{
			name:       "invalid checksum",
			have:       "01bd165db4bd480656a539e8e00db2",
			wantErr:    true,
			wantErrMsg: "invalid checksum digest length",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			got, err := tc.have.Parse()

			if tc.wantErr {
				require.Zero(tt, got)
				require.Error(tt, err)
				if tc.wantErrMsg != "" {
					require.EqualError(tt, err, tc.wantErrMsg)
				}
				return
			}
			require.NoError(tt, err)
			require.Equal(tt, tc.want, got)
		})
	}
}

func TestDigest_HexDecode(t *testing.T) {
	testCases := []struct {
		name     string
		digest   datastore.Digest
		expected string
	}{
		{
			name:     "Empty digest",
			digest:   "",
			expected: "\\x",
		},
		{
			name:     "Non-empty digest",
			digest:   "abc123",
			expected: "\\xabc123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			require.Equal(tt, tc.expected, tc.digest.HexDecode())
		})
	}
}

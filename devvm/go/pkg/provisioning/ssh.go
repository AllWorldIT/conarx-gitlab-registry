package provisioning

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/pkg/sftp"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/constants"
	"gitlab.com/gitlab-org/container-registry/devvm/go/pkg/status"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/term"
)

func EstablishSSHWithSFTP(
	ctx context.Context,
	signer ssh.Signer,
	host string,
) (*ssh.Client, *sftp.Client, error) {
	sshConn, err := EstablishSSHConn(ctx, signer, host)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect via SSH: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		_ = sshConn.Close()
		return nil, nil, fmt.Errorf("unable to establish sftp client: %w", err)
	}

	return sshConn, sftpClient, nil
}

// GetSSHAgentClient attempts to connect to the SSH agent.
// Returns nil if the agent is not available.
func GetSSHAgentClient() agent.ExtendedAgent {
	socket := os.Getenv("SSH_AUTH_SOCK")
	if socket == "" {
		return nil
	}

	conn, err := net.Dial("unix", socket)
	if err != nil {
		return nil
	}

	return agent.NewClient(conn)
}

// FindMatchingAgentSigner tries to find a signer in the SSH agent that matches
// the provided public key.
func FindMatchingAgentSigner(agentClient agent.ExtendedAgent, pubKey ssh.PublicKey) (ssh.Signer, error) {
	signers, err := agentClient.Signers()
	if err != nil {
		return nil, err
	}

	targetKeyBytes := pubKey.Marshal()
	for _, signer := range signers {
		if bytes.Equal(signer.PublicKey().Marshal(), targetKeyBytes) {
			return signer, nil
		}
	}

	return nil, fmt.Errorf("matching key not found in SSH agent")
}

// ReadSSHKeysWithAgent attempts to read SSH keys, using the SSH agent if available.
// It first tries to parse the key without a passphrase.
// If the key is encrypted, it checks if the SSH agent has the key.
// If not found in agent, it prompts for passphrase.
// Returns the signer and the public key string.
func ReadSSHKeysWithAgent(keyPath string) (ssh.Signer, string, error) {
	// nolint: gosec // helper function, we control the input
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, "", fmt.Errorf("reading private key file failed: %w", err)
	}

	// Try to parse the key without passphrase first
	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err == nil {
		pubBytes := ssh.MarshalAuthorizedKey(signer.PublicKey())
		return signer, string(pubBytes), nil
	}

	// Check if it's an encrypted key error
	var ppErr *ssh.PassphraseMissingError
	if !errors.As(err, &ppErr) {
		return nil, "", fmt.Errorf("unable to parse ssh key from %s: %w", keyPath, err)
	}

	// Key is encrypted, try to find it in the SSH agent
	agentClient := GetSSHAgentClient()
	if agentClient != nil {
		// Try to get the public key from the .pub file to match against agent
		pubKeyPath := keyPath + ".pub"
		// nolint: gosec // helper function, we control the input
		pubKeyBytes, pubErr := os.ReadFile(pubKeyPath)
		// nolint: revive // nesting reflects business logic
		if pubErr == nil {
			pubKey, _, _, _, parseErr := ssh.ParseAuthorizedKey(pubKeyBytes)
			if parseErr == nil {
				agentSigner, agentErr := FindMatchingAgentSigner(agentClient, pubKey)
				if agentErr == nil {
					// Return the public key from the .pub file
					return agentSigner, string(pubKeyBytes), nil
				}
			}
		}
	}

	// SSH agent doesn't have the key or isn't available
	// Fall back to passphrase-based decryption
	passphrase, err := ReadPasswordFromTerm()
	if err != nil {
		return nil, "", fmt.Errorf("unable to read passphrase from stdin: %w", err)
	}

	signer, err = ssh.ParsePrivateKeyWithPassphrase(pemBytes, passphrase)
	if err != nil {
		return nil, "", fmt.Errorf("unable to parse ssh key from %s with passphrase: %w", keyPath, err)
	}

	pubBytes := ssh.MarshalAuthorizedKey(signer.PublicKey())
	return signer, string(pubBytes), nil
}

func FetchStatusData(sftpClient *sftp.Client) (*status.StatusData, error) {
	f, err := sftpClient.Open(constants.StatusFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open booter status file: %w", err)
	}
	defer f.Close()

	res := new(status.StatusData)
	err = json.NewDecoder(f).Decode(res)
	if err != nil {
		return nil, fmt.Errorf("unable to decode status file: %w", err)
	}

	return res, nil
}

func UploadLicenseFile(sftpClient *sftp.Client, srcPath string) error {
	fDest, err := sftpClient.OpenFile(constants.EELicensePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return fmt.Errorf("unable to open license file on devvm for writing: %w", err)
	}
	defer fDest.Close()

	// nolint: gosec // this is a helper function, we control the input
	license, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("unable to read local license file %s: %w", srcPath, err)
	}

	_, err = fDest.Write(license)
	if err != nil {
		return fmt.Errorf("unable to write license file to remote destination: %w", err)
	}

	return nil
}

func EstablishSSHConn(
	ctx context.Context,
	signer ssh.Signer,
	host string,
) (*ssh.Client, error) {
	// Create the Signer for this private key.
	config := &ssh.ClientConfig{
		User: constants.DevvmUser,
		Auth: []ssh.AuthMethod{
			// Use the PublicKeys method for remote authentication.
			ssh.PublicKeys(signer),
		},
		// nolint: gosec // TODO(prozlach): how to extract hostkey from devvm?
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// It takes some time for VM to start, hence we retry with a time limit:
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	timeout := 60 * time.Second
	to := time.NewTimer(timeout)
	defer to.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled")
		case <-to.C:
			return nil, fmt.Errorf("timed out after %s while waiting for devvm to boot, last err: %w", timeout.String(), lastErr)
		case <-ticker.C:
			var client *ssh.Client
			client, lastErr = ssh.Dial("tcp", fmt.Sprintf("%s:22", host), config)
			if lastErr != nil {
				continue
			}
			return client, nil
		}
	}
}

func ReadPasswordFromTerm() (passphrase []byte, retErr error) {
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return nil, fmt.Errorf("unable to switch terminal to raw mode: %w", err)
	}
	defer func() {
		if err := term.Restore(int(os.Stdin.Fd()), state); err != nil {
			restoreErr := fmt.Errorf("unable to restore terminal state: %w", err)
			if retErr != nil {
				retErr = fmt.Errorf("%w; %w", retErr, restoreErr)
			} else {
				retErr = restoreErr
			}
		}
	}()

	fmt.Printf("Enter the passphrase: ")
	passphrase, err = term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return nil, fmt.Errorf("unable to read password from stdin: %w", err)
	}
	_, _ = fmt.Print("\n\r")

	return passphrase, nil
}

// GenerateECDSAKeys generates ECDSA public and private key pair with given size for SSH.
func GenerateECDSAKeys(bitSize int) (ssh.Signer, string, error) {
	privateKey, err := ecdsa.GenerateKey(curveFromLength(bitSize), rand.Reader)
	if err != nil {
		return nil, "", fmt.Errorf("unable to generate ssh key: %w", err)
	}

	publicKey, err := ssh.NewPublicKey(privateKey.Public())
	if err != nil {
		return nil, "", fmt.Errorf("unable to derrive public key from private key: %w", err)
	}
	pubBytes := ssh.MarshalAuthorizedKey(publicKey)

	sshSigner, err := ssh.NewSignerFromSigner(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("unable to convert generated private key to ssh.Signer: %w", err)
	}
	return sshSigner, string(pubBytes), nil
}

func curveFromLength(l int) elliptic.Curve {
	switch l {
	case 224:
		return elliptic.P224()
	case 256:
		return elliptic.P256()
	case 384:
		return elliptic.P384()
	case 521:
		return elliptic.P521()
	}
	return elliptic.P384()
}

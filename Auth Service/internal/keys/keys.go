package keys

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Keys struct {
	Private *rsa.PrivateKey
	Public  *rsa.PublicKey
	KID     string
}

type JWK struct {
	Kty, Use, Alg, Kid, N, E string
}

func Load(path string) (*Keys, error) {
	if path == "" {
		return nil, fmt.Errorf("AUTH_JWT_PRIVATE_KEY_PATH is not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read private key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("keys: no PEM block found in private key file")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse private key: %w", err)
	}
	privK, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("Private key is not RSA")
	}

	pub := &privK.PublicKey
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("keys: marshal public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return &Keys{Private: privK, Public: pub, KID: base64.RawURLEncoding.EncodeToString(sum[:12])}, nil
}

func (this *Keys) JWKS() []JWK {
	return []JWK{{
		Kty: "RSA",
		Use: "sig",
		Alg: "RS256",
		Kid: this.KID,
		N:   base64.RawURLEncoding.EncodeToString(this.Public.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(this.Public.E)).Bytes()),
	}}
}

func GeneratePKCS8PEM() ([]byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("keys: generate key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("keys: marshal key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func EnsureKey(path string) (bool, error) {
	if path == "" {
		return false, errors.New("keys: key path is empty")
	}
	if _, statErr := os.Stat(path); statErr == nil {
		return false, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return false, fmt.Errorf("keys: check %s: %w", path, statErr)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("keys: create %s: %w", dir, err)
	}

	pemBytes, err := GeneratePKCS8PEM()
	if err != nil {
		return false, err
	}

	tmp, err := os.CreateTemp(dir, ".private-*.tmp")
	if err != nil {
		return false, fmt.Errorf("keys: create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(pemBytes); err != nil {
		tmp.Close()
		return false, fmt.Errorf("keys: write key: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, fmt.Errorf("keys: sync key: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("keys: close key file: %w", err)
	}

	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("keys: install key: %w", err)
	}
	return true, nil
}

type K8sSecretConfig struct {
	APIBaseURL string
	Namespace  string
	Token      string
	CACertPEM  []byte
	SecretName string
	KeyName    string
}

func LoadInClusterSecretConfig(secretName, keyName string) (K8sSecretConfig, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return K8sSecretConfig{}, errors.New("keys: KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT is not set")
	}
	const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	nsBytes, err := os.ReadFile(filepath.Join(saDir, "namespace"))
	if err != nil {
		return K8sSecretConfig{}, fmt.Errorf("keys: read serviceaccount namespace: %w", err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(saDir, "token"))
	if err != nil {
		return K8sSecretConfig{}, fmt.Errorf("keys: read serviceaccount token: %w", err)
	}
	caBytes, err := os.ReadFile(filepath.Join(saDir, "ca.crt"))
	if err != nil {
		return K8sSecretConfig{}, fmt.Errorf("keys: read serviceaccount ca.crt: %w", err)
	}
	if keyName == "" {
		keyName = "private.pem"
	}
	return K8sSecretConfig{
		APIBaseURL: "https://" + net.JoinHostPort(host, port),
		Namespace:  strings.TrimSpace(string(nsBytes)),
		Token:      strings.TrimSpace(string(tokenBytes)),
		CACertPEM:  caBytes,
		SecretName: secretName,
		KeyName:    keyName,
	}, nil
}

func EnsureK8sSecret(ctx context.Context, cfg K8sSecretConfig) (bool, error) {
	if cfg.SecretName == "" {
		return false, errors.New("keys: secret name is empty")
	}
	if cfg.KeyName == "" {
		cfg.KeyName = "private.pem"
	}
	if cfg.Namespace == "" || cfg.APIBaseURL == "" || cfg.Token == "" {
		return false, errors.New("keys: incomplete Kubernetes API configuration")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cfg.CACertPEM) {
		return false, errors.New("keys: failed to parse Kubernetes API CA certificate")
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}

	baseURL := strings.TrimRight(cfg.APIBaseURL, "/")
	secretURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets/%s", baseURL, cfg.Namespace, cfg.SecretName)
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, secretURL, nil)
	if err != nil {
		return false, fmt.Errorf("keys: build get secret request: %w", err)
	}
	getReq.Header.Set("Authorization", "Bearer "+cfg.Token)
	getReq.Header.Set("Accept", "application/json")

	getResp, err := client.Do(getReq)
	if err != nil {
		return false, fmt.Errorf("keys: get secret %s: %w", cfg.SecretName, err)
	}
	getBody, _ := io.ReadAll(io.LimitReader(getResp.Body, 4096))
	getResp.Body.Close()

	if getResp.StatusCode == http.StatusOK {
		return false, nil
	}
	if getResp.StatusCode != http.StatusNotFound {
		return false, fmt.Errorf("keys: get secret %s returned status %d: %s", cfg.SecretName, getResp.StatusCode, strings.TrimSpace(string(getBody)))
	}

	pemBytes, err := GeneratePKCS8PEM()
	if err != nil {
		return false, err
	}

	payload := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name": cfg.SecretName,
			"labels": map[string]string{
				"app": "auth-service",
			},
		},
		"type": "Opaque",
		"data": map[string]string{
			cfg.KeyName: base64.StdEncoding.EncodeToString(pemBytes),
		},
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("keys: marshal secret payload: %w", err)
	}

	createURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets", baseURL, cfg.Namespace)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, createURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return false, fmt.Errorf("keys: build create secret request: %w", err)
	}
	postReq.Header.Set("Authorization", "Bearer "+cfg.Token)
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("Accept", "application/json")

	postResp, err := client.Do(postReq)
	if err != nil {
		return false, fmt.Errorf("keys: create secret %s: %w", cfg.SecretName, err)
	}
	postBody, _ := io.ReadAll(io.LimitReader(postResp.Body, 4096))
	postResp.Body.Close()

	if postResp.StatusCode == http.StatusCreated || postResp.StatusCode == http.StatusOK {
		return true, nil
	}
	if postResp.StatusCode == http.StatusConflict {
		return false, nil
	}
	return false, fmt.Errorf("keys: create secret %s returned status %d: %s", cfg.SecretName, postResp.StatusCode, strings.TrimSpace(string(postBody)))
}

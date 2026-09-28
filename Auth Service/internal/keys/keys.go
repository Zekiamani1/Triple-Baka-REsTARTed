package keys

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
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

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return false, fmt.Errorf("keys: generate key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return false, fmt.Errorf("keys: marshal key: %w", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

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

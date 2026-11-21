package keys

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestGeneratePKCS8PEMAndLoad(t *testing.T) {
	pemBytes, err := GeneratePKCS8PEM()
	if err != nil {
		t.Fatalf("GeneratePKCS8PEM failed: %v", err)
	}

	tmpPath := filepath.Join(t.TempDir(), "private.pem")
	if err := os.WriteFile(tmpPath, pemBytes, 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	loaded, err := Load(tmpPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	jwks := loaded.JWKS()
	if len(jwks) != 1 || jwks[0].Alg != "RS256" || jwks[0].Kty != "RSA" {
		t.Fatalf("unexpected JWKS: %+v", jwks)
	}
}

func TestEnsureK8sSecretCreatesWhenMissing(t *testing.T) {
	var createdBody map[string]any
	var postCount int

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("unexpected auth header: %q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/secrets/auth-keys":
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/default/secrets":
			postCount++
			if err := json.NewDecoder(r.Body).Decode(&createdBody); err != nil {
				t.Fatalf("decode body failed: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	created, err := EnsureK8sSecret(context.Background(), K8sSecretConfig{
		APIBaseURL: srv.URL,
		Namespace:  "default",
		Token:      "test-token",
		CACertPEM:  caPEM,
		SecretName: "auth-keys",
		KeyName:    "private.pem",
	})
	if err != nil {
		t.Fatalf("EnsureK8sSecret failed: %v", err)
	}
	if !created {
		t.Fatal("expected created=true")
	}
	if postCount != 1 {
		t.Fatalf("expected 1 POST, got %d", postCount)
	}

	dataMap, ok := createdBody["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data field in payload: %+v", createdBody)
	}
	encodedPEM, ok := dataMap["private.pem"].(string)
	if !ok {
		t.Fatalf("missing private.pem in secret data: %+v", dataMap)
	}
	decodedPEM, err := base64.StdEncoding.DecodeString(encodedPEM)
	if err != nil {
		t.Fatalf("base64 decode failed: %v", err)
	}
	block, _ := pem.Decode(decodedPEM)
	if block == nil || block.Type != "PRIVATE KEY" {
		t.Fatalf("expected PKCS#8 PRIVATE KEY PEM block, got %+v", block)
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		t.Fatalf("ParsePKCS8PrivateKey failed: %v", err)
	}
}

func TestEnsureK8sSecretLeavesExistingAlone(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/secrets/auth-keys" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"kind":"Secret"}`))
			return
		}
		t.Fatalf("unexpected request when secret exists: %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	created, err := EnsureK8sSecret(context.Background(), K8sSecretConfig{
		APIBaseURL: srv.URL,
		Namespace:  "default",
		Token:      "test-token",
		CACertPEM:  caPEM,
		SecretName: "auth-keys",
		KeyName:    "private.pem",
	})
	if err != nil {
		t.Fatalf("EnsureK8sSecret failed: %v", err)
	}
	if created {
		t.Fatal("expected created=false when secret already exists")
	}
}

func TestEnsureK8sSecretHandlesConflict(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusConflict)
		}
	}))
	defer srv.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	created, err := EnsureK8sSecret(context.Background(), K8sSecretConfig{
		APIBaseURL: srv.URL,
		Namespace:  "default",
		Token:      "test-token",
		CACertPEM:  caPEM,
		SecretName: "auth-keys",
		KeyName:    "private.pem",
	})
	if err != nil {
		t.Fatalf("EnsureK8sSecret failed on conflict: %v", err)
	}
	if created {
		t.Fatal("expected created=false on 409 conflict")
	}
}

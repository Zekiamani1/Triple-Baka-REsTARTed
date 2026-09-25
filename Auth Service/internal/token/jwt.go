package token

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWTService struct {
	privateKey *rsa.PrivateKey
}

func NewJWTService() (*JWTService, error) {
	privKey, err := loadPrivKey()
	if err != nil {
		return nil, err
	}
	return &JWTService{privateKey: privKey}, nil
}

func loadPrivKey() (*rsa.PrivateKey, error) {
	path := os.Getenv("AUTH_JWT_PRIVATE_KEY_PATH")
	if path == "" {
		return nil, fmt.Errorf("AUTH_JWT_PRIVATE_KEY_PATH is not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read private key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("Failed to decode PEM private key: %w", err)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse private key: %w", err)
	}
	privK, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("Private key is not RSA")
	}
	return privK, nil
}

func (this *JWTService) GenerateAccessToken(data string) (string, error) {
	ttl := os.Getenv("AUTH_REFRESH_TOKEN_TTL_SECONDS")
	ttlVal, err := strconv.Atoi(ttl)
	if err != nil {
		return "", fmt.Errorf("Error parsing string: %w", err)
	}
	claims := jwt.MapClaims{
		"sub": data,
		"exp": time.Now().Add(time.Duration(ttlVal) * time.Second).Unix(),
		"iat": time.Now().Unix(),
	}

	return jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(this.privateKey)
}

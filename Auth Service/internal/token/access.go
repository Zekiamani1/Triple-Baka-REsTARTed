package token

import (
	"os"
	"time"

	"auth-service/internal/client"
	"auth-service/internal/keys"
	"github.com/golang-jwt/jwt/v5"
)

func BuildAccessToken(key *keys.Keys, client *client.Client, ttl time.Duration) (accessToken string, expiresAt time.Time, err error) {
	exp := time.Now().Add(ttl)
	issuer := os.Getenv("AUTH_ISSUER")
	claims := jwt.MapClaims{
		"sub":   client.ClientID,
		"scope": client.Scope,
		"exp":   exp.Unix(),
		"iat":   time.Now().Unix(),
		"iss":   issuer,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = key.KID
	strtok, err := token.SignedString(key.Private)
	if err != nil {
		return "", time.Time{}, err
	}
	return strtok, exp, nil
}

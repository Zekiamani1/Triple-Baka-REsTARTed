package main

import (
	"github.com/Zekiamani1/Triple-Baka-REsTARTed/auth-service/internal/token"
	"log"
)

func main() {
	jwtService, err := token.NewJWTService()
	if err != nil {
		log.Fatal(err)
	}
	newToken, err := jwtService.GenerateAccessToken("abcd")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(newToken)
	_ = jwtService
}

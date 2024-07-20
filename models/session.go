package models

type Session struct {
	AuthenticationMethod string `json:"authentication_method"`
	Email                string `json:"email"`
	UID                  string `json:"uid"`
	User                 User   `json:"user"`
	Token                string `json:"token"`
	RefreshToken         string `json:"refresh_token"`
}

type JWTClaims struct {
	User User `json:"user"`
}

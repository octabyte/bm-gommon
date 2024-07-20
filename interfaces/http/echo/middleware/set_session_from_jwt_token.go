package middleware

import (
	"encoding/base64"
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
	"strings"
)

func SetSessionFromJWTToken() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Attempt to get the JWTToken from the request header first
			JWTToken := c.Request().Header.Get(Authorization)

			// If not present, attempt to get it from the cookie
			if JWTToken == "" {
				cookie, err := c.Cookie(SessionHeader)
				if err != nil {
					if err.Error() != "http: named cookie not present" {
						log.Errorf("Error retrieving session cookie: %v", err)
					}
					// Proceed to the next middleware if the cookie is not present or an error occurred
					return next(c)
				}
				JWTToken = cookie.Value
			}

			// Split the token
			parts := strings.Split(JWTToken, ".")
			if len(parts) != 3 {
				return next(c)
			}

			// Decode the payload (second part of the token)
			payload, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err != nil {
				return next(c)
			}

			// Parse the JSON payload
			var claims map[string]interface{}
			err = json.Unmarshal(payload, &claims)
			if err != nil {
				return next(c)
			}

			// Set the claims in the context
			c.Set(JWTSessionKey, claims)

			return next(c)
		}
	}
}

package middleware

import (
	"encoding/base64"
	"github.com/goccy/go-json"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
	"github.com/octabyte/bm-gommon/models"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
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

			userString := gjson.GetBytes(payload, "user").String()

			// Parse the JSON payload
			var user models.User
			err = json.Unmarshal([]byte(userString), &user)
			if err != nil {
				return next(c)
			}

			newContext := context.WithValue(c.Request().Context(), JWTSessionKey, user)
			c.SetRequest(c.Request().WithContext(newContext))
			return next(c)
		}
	}
}

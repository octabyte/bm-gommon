package middleware

import (
	"github.com/labstack/echo/v4"
	"golang.org/x/net/context"
	"log"
)

func SetSessionInContext() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Attempt to get the session from the request header first
			session := c.Request().Header.Get(SessionHeader)

			// If not present, attempt to get it from the cookie
			if session == "" {
				cookie, err := c.Cookie(SessionHeader)
				if err != nil {
					if err.Error() != "http: named cookie not present" {
						// Log unexpected errors
						log.Printf("Error retrieving session cookie: %v", err)
					}
					// Proceed to the next middleware if the cookie is not present or an error occurred
					return next(c)
				}
				session = cookie.Value
			}

			newContext := context.WithValue(c.Request().Context(), RequestSessionKey, session)
			c.SetRequest(c.Request().WithContext(newContext))
			return next(c)
		}
	}
}

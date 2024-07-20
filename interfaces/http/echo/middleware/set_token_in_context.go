package middleware

import (
	"context"
	"errors"
	"github.com/labstack/echo/v4"
	"log"
	"net/http"
)

func SetTokenInContext() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token := c.Request().Header.Get(Authorization)

			if token == "" {
				cookie, err := c.Cookie(Authorization)
				if err != nil {
					if !errors.Is(err, http.ErrNoCookie) {
						log.Printf("Error retrieving authorization cookie: %v", err)
					}
					return next(c)
				}
				token = cookie.Value
			}

			newContext := context.WithValue(c.Request().Context(), TokenKey, token)
			c.SetRequest(c.Request().WithContext(newContext))
			return next(c)
		}
	}
}

package middleware

import (
	"errors"
	"github.com/labstack/echo/v4"
	"log"
	"net/http"
)

const (
	TokenKey      = "requestToken"
	Authorization = "Authorization"
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

			c.Set(TokenKey, token)
			return next(c)
		}
	}
}

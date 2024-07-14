package middleware

import "github.com/labstack/echo/v4"

const (
	TokenKey = "requestToken"
)

func SetTokenInContext() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// First, look in the request header for the Authorization key
			token := c.Request().Header.Get("Authorization")

			// If no present, look in cookie
			if token == "" {
				cookie, err := c.Cookie("Authorization")
				if err == nil {
					token = cookie.Value
				}
			}

			c.Set(TokenKey, token)
			return next(c)
		}
	}
}

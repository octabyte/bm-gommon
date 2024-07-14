package middleware

import "github.com/labstack/echo/v4"

func SetSessionInContext() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// First, look in the request header for the Authorization key
			session := c.Request().Header.Get("Session")

			// If no present, look in cookie
			if session == "" {
				cookie, err := c.Cookie("Session")
				if err == nil {
					session = cookie.Value
				}
			}

			c.Set("requestSession", session)
			return next(c)
		}
	}
}

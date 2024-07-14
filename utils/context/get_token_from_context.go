package context

import "github.com/labstack/echo/v4"

func GetTokenFromContext(c echo.Context) string {
	return c.Get("requestToken").(string)
}

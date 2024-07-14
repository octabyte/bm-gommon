package context

import "github.com/labstack/echo/v4"

func GetSessionFromContext(c echo.Context) string {
	return c.Get("requestSession").(string)
}

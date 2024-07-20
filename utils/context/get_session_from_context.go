package context

import (
	"github.com/octabyte/bm-gommon/models"
	"golang.org/x/net/context"
)

func GetSessionFromContext(ctx context.Context) models.Session {
	return ctx.Value("jwtSession").(models.Session)
}

func GetUserFromContext(ctx context.Context) models.User {
	user := ctx.Value("jwtSession").(models.User)

	return user
}

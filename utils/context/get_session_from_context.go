package context

import (
	"context"

	"github.com/octabyte/bm-gommon/models"
)

func GetSessionFromContext(ctx context.Context) models.Session {
	return ctx.Value("jwtSession").(models.Session)
}

func GetUserFromContext(ctx context.Context) models.User {
	user := ctx.Value("jwtSession").(models.User)

	return user
}

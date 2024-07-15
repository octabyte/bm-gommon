package context

import (
	"github.com/octabyte/bm-gommon/models"
	"golang.org/x/net/context"
)

func GetSessionFromContext(ctx context.Context) models.Session {
	return ctx.Value("requestSession").(models.Session)
}

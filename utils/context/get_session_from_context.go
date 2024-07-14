package context

import (
	"golang.org/x/net/context"
)

func GetSessionFromContext(ctx context.Context) string {
	return ctx.Value("requestSession").(string)
}

package context

import (
	"context"
)

func GetTokenFromContext(ctx context.Context) string {
	return ctx.Value("requestToken").(string)
}

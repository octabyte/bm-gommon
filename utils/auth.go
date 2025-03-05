package utils

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/tidwall/gjson"
)

type Config struct {
	Host     string
	Store    string
	ApiToken string
}

func GetNewestAuthorizationModelID(config *Config) (string, error) {
	client := resty.New()
	client.BaseURL = config.Host

	if config.Store == "" {
		return "", fmt.Errorf("store is required")
	}

	resp, err := client.R().
		SetQueryParams(map[string]string{
			"page_size": "1",
		}).
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", config.ApiToken)).
		Get(fmt.Sprintf("/stores/%s/authorization-models", config.Store))
	if err != nil {
		return "", err
	}

	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to get newest authorization model id")
	}

	authorizationModelID := gjson.Get(resp.String(), "authorization_models.0.id").String()
	if authorizationModelID == "" {
		return "", fmt.Errorf("failed to get newest authorization model id")
	}

	return authorizationModelID, nil
}

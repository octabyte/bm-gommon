package utils

import "github.com/goccy/go-json"

func StructToBytes(s interface{}) ([]byte, error) {
	return json.Marshal(s)
}

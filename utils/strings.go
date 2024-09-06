package utils

import "github.com/goccy/go-json"

func StructToBytes(s interface{}) ([]byte, error) {
	return json.Marshal(s)
}

func BytesToStruct(data []byte, s interface{}) error {
	return json.Unmarshal(data, s)
}

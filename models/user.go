package models

type User struct {
	ID             uint64 `json:"id"`
	UID            string `json:"uid"`
	FirstName      string `json:"first_name"`
	SecondName     string `json:"second_name,omitempty"`
	LastName       string `json:"last_name"`
	SecondLastName string `json:"second_last_name,omitempty"`
	Email          string `json:"email,omitempty"`
}

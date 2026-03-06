package uuidutil

import "github.com/google/uuid"

func NewV7() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
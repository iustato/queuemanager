package uuidutil

import "github.com/google/uuid"

// NewV7 returns a RFC 4122 UUID version 7.
func NewV7() string {
	u, err := uuid.NewV7()
	if err != nil {
		// теоретически почти невозможно, но пусть будет fail-fast
		panic(err)
	}
	return u.String()
}

package main

import (
"fmt"
"github.com/google/uuid"
)

func main() {
u, _ := uuid.NewV7()
fmt.Println(u.String())
}

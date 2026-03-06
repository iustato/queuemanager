//go:build !unix

package queue

import "os/exec"

// На не-unix платформах (windows и т.п.) делаем no-op.
// Это гарантирует сборку и предсказуемое поведение.
func makeProcessGroup(cmd *exec.Cmd) {}

func killProcessGroup(cmd *exec.Cmd) {}
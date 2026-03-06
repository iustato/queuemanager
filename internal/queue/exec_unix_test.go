//go:build unix

package queue

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// Этот тест проверяет, что makeProcessGroup не падает и реально назначает pgid.
func TestMakeProcessGroup_SetsPgid(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 2")
	makeProcessGroup(cmd)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	time.Sleep(50 * time.Millisecond)

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatalf("getpgid: %v", err)
	}
	// Обычно pgid == pid, если мы создали новую группу процессов
	if pgid != cmd.Process.Pid {
		t.Fatalf("pgid: got %d want %d", pgid, cmd.Process.Pid)
	}
	_ = cmd.Wait()
}

// Этот тест проверяет, что killProcessGroup убивает процесс (и группу).
func TestKillProcessGroup_Kills(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 10")
	makeProcessGroup(cmd)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// дать стартануть
	time.Sleep(50 * time.Millisecond)

	killProcessGroup(cmd)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("expected process to exit after killProcessGroup")
	case err := <-done:
		// err может быть != nil из-за сигнала — это нормально
		_ = err
	}
}
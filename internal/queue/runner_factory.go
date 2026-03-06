package queue

import (
	"fmt"
	"strings"
	"time"
)

// RunnerFactory creates one Runner instance (you create one per worker).
type RunnerFactory func() (Runner, error)

// DefaultRunnerFactory returns a factory based on runtime settings.
// It closes over rt (kind/fpm/script/command).
func DefaultRunnerFactory(rt *Runtime) RunnerFactory {
	return func() (Runner, error) {
		switch rt.Kind {
		case RuntimePHPFPM:
			netw := strings.TrimSpace(rt.FPMNetwork)
			addr := strings.TrimSpace(rt.FPMAddress)

			if netw == "" {
				netw = "unix"
			}
			if addr == "" {
				return nil, fmt.Errorf("FPMAddress is required for php-fpm")
			}

			return &PooledFastCGIRunner{
				Network:          netw,
				Address:          addr,
				ServerName:       "queue-service",
				ServerPort:       "80",
				DialTimeout:      3 * time.Second,
				DocumentRoot:     "",
				MaxResponseBytes: 4 << 20, // 4 MiB
				TruncateOnLimit:  false,
			}, nil

		case RuntimePHPCGI:
			return PHPCGIRunner{
				MaxStdoutBytes:   4 << 20,
				MaxStderrBytes:   1 << 20,
				KillProcessGroup: true,
			}, nil

		case RuntimeExec:
			fallthrough
		default:
			if len(rt.Command) == 0 {
				return nil, fmt.Errorf("command is required for exec runtime")
			}
			return ExecRunner{
				MaxStdoutBytes:   4 << 20,
				MaxStderrBytes:   1 << 20,
				KillProcessGroup: true,
			}, nil
		}
	}
}
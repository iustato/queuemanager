package queue

import (
	"fmt"
	"strings"
	"time"
)

// RunnerFactory creates one Runner instance (you create one per worker).
type RunnerFactory func() (Runner, error)

type RuntimeOption func(*runtimeOptions)

type runtimeOptions struct {
	runnerFactory RunnerFactory
	phpCgiBin     string
}

func WithRunnerFactory(f RunnerFactory) RuntimeOption {
	return func(o *runtimeOptions) { o.runnerFactory = f }
}

func WithPhpCgiBin(bin string) RuntimeOption {
	return func(o *runtimeOptions) { o.phpCgiBin = bin }
}

// DefaultRunnerFactory returns a RunnerFactory that creates the appropriate
// Runner based on rt.Kind (php-fpm, php-cgi, exec).
func DefaultRunnerFactory(rt *Runtime, phpCgiBin string) RunnerFactory {
	cfg := rt.Cfg

	truncate := false
	if cfg.TruncateOnLimit != nil {
		truncate = *cfg.TruncateOnLimit
	}

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
				ServerName:       cfg.FPMServerName,
				ServerPort:       cfg.FPMServerPort,
				DialTimeout:      time.Duration(cfg.FPMDialTimeoutMs) * time.Millisecond,
				DocumentRoot:     "",
				MaxResponseBytes: int64(cfg.MaxResponseBytes),
				TruncateOnLimit:  truncate,
			}, nil

		case RuntimePHPCGI:
			return PHPCGIRunner{
				PhpCgiBin:        phpCgiBin,
				MaxStdoutBytes:   cfg.MaxStdoutBytes,
				MaxStderrBytes:   cfg.MaxStderrBytes,
				KillProcessGroup: true,
			}, nil

		case RuntimeExec:
			fallthrough
		default:
			if len(rt.Command) == 0 {
				return nil, fmt.Errorf("command is required for exec runtime")
			}

			return ExecRunner{
				MaxStdoutBytes:   cfg.MaxStdoutBytes,
				MaxStderrBytes:   cfg.MaxStderrBytes,
				KillProcessGroup: true,
			}, nil
		}
	}
}

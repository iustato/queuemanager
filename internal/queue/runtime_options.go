package queue

// RunnerFactory creates one Runner instance (you create one per worker).
type RunnerFactory func() (Runner, error)

type RuntimeOption func(*runtimeOptions)

type runtimeOptions struct {
	runnerFactory RunnerFactory
}

func WithRunnerFactory(f RunnerFactory) RuntimeOption {
	return func(o *runtimeOptions) { o.runnerFactory = f }
}
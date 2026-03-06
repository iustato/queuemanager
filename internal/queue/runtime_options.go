package queue

type RuntimeOption func(*runtimeOptions)

type runtimeOptions struct {
	runnerFactory RunnerFactory
}

func WithRunnerFactory(f RunnerFactory) RuntimeOption {
	return func(o *runtimeOptions) { o.runnerFactory = f }
}
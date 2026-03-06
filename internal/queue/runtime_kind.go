package queue

type RuntimeKind string

const (
	RuntimePHPFPM RuntimeKind = "php-fpm"
	RuntimePHPCGI RuntimeKind = "php-cgi"
	RuntimeExec   RuntimeKind = "exec"
)
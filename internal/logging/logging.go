package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewProductionLogger — JSON логи для прод/серверов.
func NewProductionLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig() // разумный production-дефолт (JSON, уровни, stacktrace на error+)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Пример: можно добавить sampling, если логов очень много
	// cfg.Sampling = &zap.SamplingConfig{Initial: 100, Thereafter: 100}

	return cfg.Build()
}

// NewDevelopmentLogger — читабельные логи для локальной разработки.
func NewDevelopmentLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg.Build()
}

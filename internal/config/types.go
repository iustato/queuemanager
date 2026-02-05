package config
// QueueConfig описывает конфигурацию ОДНОЙ очереди,

type QueueConfig struct {
	// Name — логическое имя очереди.
	//
	// Используется:
	//  - в URL: /{queue}/newmessage
	//  - как ключ в map очередей
	//
	// Если не задано явно в YAML —
	// будет выведено из имени файла.
	Name string `yaml:"name"`

	// SchemaFile — путь к JSON Schema файла,
	// по которому валидируются входящие сообщения.
	//
	// Может быть относительным (относительно директории конфигов)
	// или абсолютным — loader приводит к абсолютному виду.
	SchemaFile string `yaml:"schema_file"`

	MaxSize int `yaml:"max_size"`

	Workers int `yaml:"workers"`
}

// AppConfig (опционально) — конфигурация всего приложения.

type AppConfig struct {
	// QueuesDir — директория, из которой
	// загружаются YAML-конфиги очередей.
	QueuesDir string
}

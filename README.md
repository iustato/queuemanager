# Queue Service

Лёгкий сервис очередей для асинхронной обработки задач через HTTP API.
Поддерживает валидацию сообщений, идемпотентность, retry-механизм и персистентное хранение задач.

Сервис принимает задачи по HTTP, сохраняет их в BoltDB и обрабатывает воркерами.

---

# Возможности

* HTTP API для добавления задач
* JSON Schema валидация сообщений
* Идемпотентность запросов (`Idempotency-Key`)
* Retry политика
* Получение статуса и результата выполнения
* Поддержка нескольких типов runtime:

  * `php-cgi`
  * `php-fpm`
  * `exec`
* Персистентное хранение сообщений (BoltDB)
* Параллельная обработка воркерами

---

# Архитектура

```
Client
  │
  │ HTTP POST /{queue}/newmessage
  ▼
Queue Service
  │
  ├── Storage (BoltDB)
  │
  ├── Runtime
  │      ├ workers
  │      └ retry scheduler
  │
  └── Worker script (PHP / exec)
```

---

# Запуск сервиса

## Требования

* Go
* PHP (если используется `php-cgi` или `php-fpm`)
* Директория конфигов очередей

## Переменные окружения

| Переменная               | Описание                          |
| ------------------------ | --------------------------------- |
| `QUEUES_CONFIG_DIR`      | директория YAML конфигов очередей |
| `QUEUE_STORAGE_PATH`     | путь к BoltDB                     |
| `PUBLIC_ADDR`            | публичный HTTP адрес              |
| `INTERNAL_ADDR`          | внутренний HTTP адрес             |
| `ALLOW_AUTO_IDEMPOTENCY` | автогенерация Idempotency-Key     |

## Пример запуска

```bash
ALLOW_AUTO_IDEMPOTENCY=true \
QUEUES_CONFIG_DIR=./configs \
QUEUE_STORAGE_PATH=./data/queue.db \
go run ./cmd/server
```

Проверка:

```bash
curl http://localhost:8080/health
```

---

# Конфигурация очереди

Каждая очередь описывается YAML файлом.

Пример:

`configs/generate_report.yaml`

```yaml
name: generate_report
schema_file: generate_report.schema.json

max_size: 2048
workers: 5

runtime: php-cgi
script: scripts/generateReport.php

timeout_sec: 60
max_queue: 50

max_retries: 3
retry_delay_ms: 2000
```

## Параметры

| Поле             | Описание                                   |
| ---------------- | ------------------------------------------ |
| `name`           | имя очереди                                |
| `schema_file`    | JSON схема сообщения                       |
| `max_size`       | максимальный размер сообщения              |
| `workers`        | количество воркеров                        |
| `runtime`        | тип runtime (`php-cgi`, `php-fpm`, `exec`) |
| `script`         | путь к worker скрипту                      |
| `timeout_sec`    | максимальное время выполнения              |
| `max_queue`      | максимальный размер очереди                |
| `max_retries`    | количество retry                           |
| `retry_delay_ms` | задержка перед retry                       |

---

# JSON Schema сообщений

Все сообщения валидируются перед постановкой в очередь.

Пример:

`generate_report.schema.json`

```json
{
  "type": "object",
  "required": ["report_type", "period"],
  "properties": {
    "report_type": {
      "type": "string",
      "enum": ["transactions","balances","audit"]
    },
    "period": {
      "type": "object",
      "required": ["from","to"]
    }
  }
}
```

Если JSON не соответствует схеме — возвращается ошибка `400`.

---

# Добавление задачи

Endpoint:

```
POST /{queue}/newmessage
```

## Пример

```bash
curl -i -X POST "http://localhost:8080/generate_report/newmessage" \
  -H "Content-Type: application/json" \
  -d '{
    "report_type": "transactions",
    "period": {
      "from": "2025-01-01T00:00:00Z",
      "to": "2025-01-31T23:59:59Z"
    },
    "format": "pdf",
    "filters": {
      "currency": ["EUR","USD"],
      "min_amount": 10
    },
    "delivery": {
      "type": "email",
      "email": "user@example.com"
    }
  }'
```

Ответ:

```
HTTP/1.1 202 Accepted
X-Message-Id: <msg_id>
Idempotency-Key: <key>
X-Request-Id: <request_id>
```

---

# Проверка статуса задачи

```
GET /{queue}/status/{msg_id}
```

Пример:

```bash
curl http://localhost:8080/generate_report/status/<msg_id>
```

Возможные статусы:

```
created
processing
succeeded
failed
```

---

# Получение результата

```
GET /{queue}/result/{msg_id}
```

Пример:

```bash
curl http://localhost:8080/generate_report/result/<msg_id>
```

---

# Retry механизм

Если воркер завершился с ошибкой:

```
exit_code != 0
```

или произошёл timeout:

```
context deadline exceeded
```

задача будет повторно поставлена в очередь до `max_retries`.

---

# Логи выполнения

stdout/stderr воркера сохраняются в:

```
configs/scripts/logs/<queue>/<msg_id>.log
```

Лог содержит:

```
ATTEMPT 1
ATTEMPT 2
...
```

---

# Пример использования: генерация отчётов

1. Клиент отправляет запрос:

```
POST /generate_report/newmessage
```

2. Сервис кладёт задачу в очередь

3. Worker выполняет скрипт:

```
scripts/generateReport.php
```

4. Результат можно получить:

```
GET /generate_report/result/{msg_id}
```

---

# Типичные ошибки

### Queue busy

```
429 queue_busy
```

Очередь переполнена.

---

### Schema validation error

```
400 invalid_schema
```

JSON не соответствует схеме.

---

### Idempotency conflict

```
409 duplicate_message
```

Сообщение уже было отправлено.

---

# Применение

Сервис можно использовать для:

* генерации отчётов
* обработки email
* фоновых задач
* обработки webhook
* интеграции с внешними сервисами

---

# Queue Service

Лёгкий сервис очередей для асинхронной обработки задач через HTTP API.
Поддерживает валидацию сообщений, дедупликацию через MessageGUID, retry-механизм и персистентное хранение задач.

Сервис принимает задачи по HTTP, сохраняет их в BoltDB и обрабатывает воркерами.

---

# Возможности

* HTTP API для добавления задач
* JSON Schema валидация сообщений
* Дедупликация через `MessageGUID` (UUIDv7)
* Retry политика
* Получение статуса и результата выполнения
* Воркер возвращает результат через structured stdout
* Эндпоинт для получения JSON Schema очереди
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
         └── stdout → Result.Output
```

---

# Запуск сервиса

## Требования

* Go
* PHP (если используется `php-cgi` или `php-fpm`)
* Директория конфигов очередей

## Переменные окружения

| Переменная               | Описание                                                   |
| ------------------------ | ---------------------------------------------------------- |
| `QUEUES_CONFIG_DIR`      | директория YAML конфигов очередей                          |
| `QUEUE_STORAGE_DIR`      | директория хранения BoltDB (per-queue)                     |
| `PUBLIC_ADDR`            | публичный HTTP адрес (default `0.0.0.0:8080`)              |
| `INTERNAL_ADDR`          | внутренний HTTP адрес (default `127.0.0.1:8081`)           |
| `ALLOW_AUTO_GUID`        | автогенерация MessageGUID если заголовок не передан        |
| `WORKER_TOKEN`           | токен для авторизации внутренних воркер-коллбэков          |

## Пример запуска

```bash
ALLOW_AUTO_GUID=true \
QUEUES_CONFIG_DIR=./configs \
QUEUE_STORAGE_DIR=./data \
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

result_ttl: "24h"
```

## Параметры

| Поле             | Описание                                   |
| ---------------- | ------------------------------------------ |
| `name`           | имя очереди                                |
| `schema_file`    | путь к JSON Schema файлу                   |
| `max_size`       | максимальный размер тела сообщения (байт)  |
| `workers`        | количество воркеров                        |
| `runtime`        | тип runtime (`php-cgi`, `php-fpm`, `exec`) |
| `script`         | путь к worker скрипту                      |
| `timeout_sec`    | максимальное время выполнения              |
| `max_queue`      | максимальный размер очереди                |
| `max_retries`    | количество retry                           |
| `retry_delay_ms` | задержка перед retry (мс)                  |
| `result_ttl`     | TTL хранения результата (`24h`, `30m`, …)  |
| `allow_auto_guid`| автогенерация GUID на уровне очереди       |

---

# JSON Schema сообщений

Все сообщения валидируются перед постановкой в очередь.

Пример:

`generate_report.schema.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["report_type", "period"],
  "properties": {
    "report_type": {
      "type": "string",
      "enum": ["transactions", "balances", "audit"]
    },
    "period": {
      "type": "object",
      "required": ["from", "to"]
    }
  },
  "additionalProperties": false
}
```

Если JSON не соответствует схеме — возвращается `422 schema_validation_failed`.

---

# Добавление задачи

```
POST /{queue}/newmessage
```

## MessageGUID

Каждое сообщение идентифицируется уникальным `MessageGUID` (UUIDv7). Он обеспечивает дедупликацию: повторный запрос с тем же GUID не создаёт дубль.

Передаётся заголовком:

```
X-Message-GUID: 019503a0-7e2a-7000-8000-000000000001
```

**Требования к формату:** валидный **UUIDv7** (RFC 9562). Другие версии UUID или произвольные строки не принимаются — сервер вернёт `400 invalid_message_guid`.

Если `ALLOW_AUTO_GUID=true` — GUID генерируется автоматически при отсутствии заголовка.

## Пример запроса

```bash
curl -X POST "http://localhost:8080/generate_report/newmessage" \
  -H "Content-Type: application/json" \
  -H "X-Message-GUID: 019503a0-7e2a-7000-8000-000000000001" \
  -d '{
    "report_type": "transactions",
    "period": {
      "from": "2025-01-01T00:00:00Z",
      "to":   "2025-01-31T23:59:59Z"
    }
  }'
```

## Ответ

```json
{
  "ok": true,
  "data": {
    "status": "accepted",
    "message_guid": "019503a0-7e2a-7000-8000-000000000001",
    "created_at": "2025-01-15T12:00:00Z"
  }
}
```

Заголовок ответа: `X-Message-GUID: <guid>`

Если сообщение с таким GUID уже существует — возвращается тот же `message_guid` с `"status": "duplicate"` (дедупликация прозрачна, не является ошибкой).

---

# Получение JSON Schema очереди

```
GET /{queue}/schema
```

Возвращает JSON Schema, которая используется для валидации сообщений этой очереди.

```bash
curl http://localhost:8080/generate_report/schema | jq .
```

Ответ:

```json
{
  "ok": true,
  "data": {
    "queue": "generate_report",
    "schema": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      ...
    }
  }
}
```

---

# Проверка статуса задачи

```
GET /{queue}/status/{guid}
```

```bash
curl http://localhost:8080/generate_report/status/019503a0-7e2a-7000-8000-000000000001
```

Ответ:

```json
{
  "ok": true,
  "data": {
    "status": "processing"
  }
}
```

Возможные статусы:

| Статус     | Описание                          |
| ---------- | --------------------------------- |
| `created`  | сохранено, ожидает постановки     |
| `queued`   | стоит в очереди                   |
| `processing` | выполняется воркером            |
| `succeeded` | выполнено успешно                |
| `failed`   | завершилось с ошибкой             |

---

# Получение результата

```
GET /{queue}/result/{guid}
```

```bash
curl http://localhost:8080/generate_report/result/019503a0-7e2a-7000-8000-000000000001
```

Ответ:

```json
{
  "ok": true,
  "data": {
    "status": "succeeded",
    "result": {
      "exit_code": 0,
      "duration_ms": 142,
      "finished_at_ms": 1737000142000,
      "output": "Report generated: 1500 transactions"
    }
  }
}
```

Поле `output` содержит то, что воркер записал в stdout (см. раздел [Worker контракт](#worker-контракт)).

---

# Worker контракт

stdout воркера — официальный канал ответа сервису.

Воркер пишет в stdout JSON:

```json
{
  "output": "любой текст или данные — попадёт в GET /result",
  "body":   { "новое": "тело задачи" }
}
```

| Поле     | Описание                                                                |
| -------- | ----------------------------------------------------------------------- |
| `output` | Сохраняется в BoltDB, возвращается через `GET /{queue}/result/{guid}`   |
| `body`   | Заменяет тело задачи в хранилище — следующий retry получит новое тело   |

**stderr** — только лог, нигде не сохраняется.

**exit code:**
- `0` → `succeeded`
- `!= 0` → `failed`

**Fallback:** если stdout не является валидным JSON — весь вывод сохраняется как `output` (raw text).

## Примеры

**Полный контракт (exec runtime):**

```bash
#!/bin/bash
echo '{"output":"processed ok","body":{"step":2,"enriched":true}}'
exit 0
```

**Только output:**

```bash
echo '{"output":"done"}'
exit 0
```

**PHP пример:**

```php
<?php
$body = json_decode(file_get_contents('php://stdin'), true);
// ... обработка ...
echo json_encode(['output' => 'processed: ' . $body['report_type']]);
exit(0);
```

---

# Retry механизм

Если воркер завершился с ненулевым exit code или произошёл timeout — задача повторно ставится в очередь до `max_retries` раз с задержкой `retry_delay_ms`.

При retry воркер получает обновлённое тело задачи (если предыдущий запуск вернул `body` в stdout).

---

# Логирование

`stderr` воркера сохраняется в лог-файл:

```
{log_dir}/{queue}/{guid}.log
```

`stdout` воркера — это **результат задачи**, а не лог. Он сохраняется в BoltDB и доступен через API.

---

# Типичные ошибки

### Queue busy

```
429 queue_busy
```

Очередь переполнена (`max_queue` достигнут).

---

### Schema validation error

```
422 schema_validation_failed
```

Тело сообщения не соответствует JSON Schema очереди.

---

### Invalid MessageGUID

```
400 invalid_message_guid
```

Заголовок `X-Message-GUID` отсутствует (при `ALLOW_AUTO_GUID=false`) или содержит не UUIDv7.

---

### Queue not found

```
404 queue_not_found
```

Очередь с таким именем не зарегистрирована.

---

# Admin API

Помимо YAML-конфигов, очередями можно управлять через HTTP API.

| Метод    | Путь                  | Описание              |
| -------- | --------------------- | --------------------- |
| `GET`    | `/api/queues`         | список всех очередей  |
| `POST`   | `/api/queues`         | создать очередь       |
| `PUT`    | `/api/queues/{name}`  | обновить очередь      |
| `DELETE` | `/api/queues/{name}`  | удалить очередь       |

Пример создания очереди с `exec` runtime:

```bash
curl -X POST http://localhost:8080/api/queues \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "name": "my-queue",
      "runtime": "exec",
      "command": ["bash", "-c", "echo \"{\\\"output\\\":\\\"done\\\"}\""],
      "workers": 2,
      "timeout_sec": 10,
      "result_ttl": "1h"
    },
    "schema": { "type": "object" }
  }'
```

> **Важно:** очереди, созданные через Admin API, хранятся только в памяти и исчезают при перезапуске сервиса. Для постоянных очередей используйте YAML-конфиги.

---

# Применение

Сервис можно использовать для:

* генерации отчётов
* обработки email
* фоновых задач
* обработки webhook
* интеграции с внешними сервисами

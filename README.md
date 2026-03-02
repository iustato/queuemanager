Queue Service

Лёгкий и расширяемый сервис очередей для асинхронной обработки задач через HTTP API с поддержкой идемпотентности, повторных попыток (Retry Policy) и персистентного хранения на BoltDB.

📌 Назначение

Сервис предназначен для приёма задач по HTTP, постановки их в очередь и последующей обработки воркерами с возможностью повторного выполнения при ошибках.

Подходит для:

асинхронной обработки бизнес-логики,

интеграции с PHP-скриптами,

фоновых задач (уведомления, обработка данных, вебхуки),

задач с требованием идемпотентности.

🏗 Архитектура

Система построена по модульному принципу:

HTTP API слой — принимает и валидирует входящие запросы

Manager — управляет жизненным циклом очередей

Runtime — среда выполнения задач конкретной очереди

Workers — конкурентная обработка сообщений

Storage (BoltDB) — персистентное хранение сообщений и метаданных

Runner — стратегия выполнения (Exec / FastCGI)

⚙️ Основные возможности

Приём задач через HTTP

Валидация JSON по схеме

Идемпотентность через Idempotency-Key

Retry Policy (MaxRetries + RetryDelay)

Статусы задач:

created

processing

done

failed

Хранение:

body

meta

idempotency mapping

result

timestamps (created_at, updated_at)

Получение статуса и результата выполнения

Логирование через Zap

Поддержка нескольких очередей

📂 Структура хранения (BoltDB)

Используются следующие bucket’ы:

meta:    msg_id -> JSON(meta)
body:    msg_id -> raw payload
result:  msg_id -> JSON(result)
idem:    idempotency_key -> msg_id
exp:     expiresAt|msg_id -> msg_id
🔁 Retry Policy

Если задача завершилась ошибкой (ExitCode != 0 или timeout):

Увеличивается счётчик Attempt

Если Attempt < MaxRetries — задача повторно ставится в очередь

Между попытками соблюдается RetryDelayMs

После превышения лимита — статус failed

🌐 HTTP API
POST /{queue}/newmessage

Создание новой задачи.

Headers:
Content-Type: application/json
Idempotency-Key: optional-unique-key
Body:
{
  "text": "example"
}
Ответ:
{
  "msg_id": "uuid-v7",
  "status": "created"
}
GET /{queue}/status/{msg_id}

Получение статуса и результата задачи.

GET /{queue}/info

Получение информации об очереди:

количество задач

статистика за последние 24 часа

🧠 Идемпотентность

Если передан Idempotency-Key:

При повторном запросе с тем же ключом будет возвращён существующий msg_id

Новая задача создана не будет

Это предотвращает дублирование при сетевых сбоях и повторных HTTP-запросах.

🛠 Конфигурация очереди (YAML)

Пример:

name: queue1
workers: 3
max_queue: 100
timeout_sec: 30
max_retries: 5
retry_delay_ms: 2000
runtime: php-cgi
script: ./configs/scripts/job.php
schema: ./configs/queue1.schema.json
🚀 Запуск
go run ./cmd/server

Или с переменными окружения:

ALLOW_AUTO_IDEMPOTENCY=true go run ./cmd/server
🔐 Безопасность

Валидация входного JSON

Ограничение размера тела запроса

Контроль HTTP методов

Изоляция выполнения скриптов

Логирование ошибок

📈 Масштабируемость

Конкурентные worker’ы

Возможность запуска нескольких экземпляров сервиса

Гибкая конфигурация очередей

Разделение API и Runtime

🧩 Используемые технологии

Go

BoltDB (bbolt)

Zap Logger

UUID v7

PHP (через ExecRunner / FastCGIRunner)

📊 Жизненный цикл задачи
HTTP Request
   ↓
Validation
   ↓
Store (created)
   ↓
Enqueue
   ↓
Worker
   ↓
Runner (exec/php)
   ↓
Store result
   ↓
done / retry / failed
📄 Логирование

Логи структурированы (JSON):

queue_registered

queue_started

job_enqueued

job_started

job_done

retry_scheduled

job_failed

🎯 Цель проекта

Создание надёжного, отказоустойчивого сервиса очередей с поддержкой идемпотентности и повторных попыток выполнения задач для критичных бизнес-процессов.
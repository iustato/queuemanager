<?php
// configs/scripts/job.php

$logDir = __DIR__ . '/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0777, true);
}

$ts  = date('Y-m-d_H-i-s');
$pid = getmypid();

$msgId   = getenv('MSG_ID') ?: 'no_msg_id';
$reqId   = getenv('REQUEST_ID') ?: 'no_request_id';
$worker  = getenv('WORKER_ID') ?: 'no_worker_id';
$attempt = getenv('ATTEMPT') ?: 'no_attempt';

$logFile = sprintf(
    '%s/job_%s_pid%d_%s.log',
    $logDir,
    $ts,
    $pid,
    $msgId
);

function log_block(string $file, string $title, $data): void
{
    file_put_contents(
        $file,
        "=== $title ===\n" . var_export($data, true) . "\n\n",
        FILE_APPEND
    );
}

// marker start
file_put_contents($logFile, "=== JOB START ===\n", FILE_APPEND);

// GET / POST (JSON body не попадёт в $_POST — это нормально)
log_block($logFile, 'GET', $_GET);
log_block($logFile, 'POST', $_POST);

$body = file_get_contents('php://input');
log_block($logFile, 'BODY (raw)', $body);
log_block($logFile, 'BODY (len)', is_string($body) ? strlen($body) : null);

// decode json from body
$decoded = json_decode($body, true);
log_block($logFile, 'BODY (json_decode)', $decoded);
log_block($logFile, 'BODY (json_error)', json_last_error_msg());

// SERVER (полезные поля)
log_block($logFile, 'SERVER', [
    'REQUEST_METHOD'     => $_SERVER['REQUEST_METHOD'] ?? null,
    'SCRIPT_FILENAME'    => $_SERVER['SCRIPT_FILENAME'] ?? null,
    'QUERY_STRING'       => $_SERVER['QUERY_STRING'] ?? null,
    'REMOTE_ADDR'        => $_SERVER['REMOTE_ADDR'] ?? null,
    'CONTENT_TYPE'       => $_SERVER['CONTENT_TYPE'] ?? null,
    'CONTENT_LENGTH'     => $_SERVER['CONTENT_LENGTH'] ?? null,

    // Idempotency
    'IDEMPOTENCY_KEY'    => $_SERVER['HTTP_IDEMPOTENCY_KEY'] ?? null,

    // системный id сообщения
    'MSG_ID'             => $_SERVER['MSG_ID'] ?? getenv('MSG_ID') ?? null,
]);


// ENV — ключевая часть теста
log_block($logFile, 'ENV', getenv());

// async simulation
$sleepSec = (int)(getenv('SLEEP_SEC') ?: 30);
file_put_contents($logFile, "sleep_sec=$sleepSec\n", FILE_APPEND);

sleep($sleepSec);

// marker done
file_put_contents($logFile, "=== JOB DONE ===\n", FILE_APPEND);

exit(0);

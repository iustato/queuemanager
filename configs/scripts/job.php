<?php
// configs/scripts/job.php
// Универсальный скрипт для любых очередей (queue1/queue2/...):
// - пишет логи в configs/scripts/logs/<queue>/
// - логирует GET/POST/raw body/json decode/$_SERVER/getenv()
// - поддерживает SLEEP_SEC для имитации долгой работы
// - работает и в php-cgi, и в php-fpm

$queue = getenv('QUEUE') ?: ($_SERVER['QUEUE'] ?? 'no_queue');
$msgId = getenv('MSG_ID') ?: ($_SERVER['MSG_ID'] ?? 'no_msg_id');

$logRoot = __DIR__ . '/logs';
$logDir  = $logRoot . '/' . preg_replace('/[^a-zA-Z0-9_.-]/', '_', $queue);

if (!is_dir($logDir)) {
    @mkdir($logDir, 0777, true);
}

$ts  = date('Y-m-d_H-i-s');
$pid = getmypid();

$logFile = sprintf(
    '%s/job_%s_pid%d_%s.log',
    $logDir,
    $ts,
    $pid,
    preg_replace('/[^a-zA-Z0-9_.-]/', '_', $msgId)
);

function log_block(string $file, string $title, $data): void
{
    file_put_contents(
        $file,
        "=== $title ===\n" . var_export($data, true) . "\n\n",
        FILE_APPEND
    );
}

file_put_contents($logFile, "=== JOB START ===\n", FILE_APPEND);

// входные данные
log_block($logFile, 'META', [
    'QUEUE'    => $queue,
    'MSG_ID'   => $msgId,
    'PID'      => $pid,
    'TS'       => $ts,
]);

// GET / POST (JSON body не попадает в $_POST — это норм)
log_block($logFile, 'GET', $_GET);
log_block($logFile, 'POST', $_POST);

// raw body
$body = file_get_contents('php://input');
log_block($logFile, 'BODY (raw)', $body);
log_block($logFile, 'BODY (len)', is_string($body) ? strlen($body) : null);

// decode json
$decoded = json_decode($body, true);
log_block($logFile, 'BODY (json_decode)', $decoded);
log_block($logFile, 'BODY (json_error)', json_last_error_msg());

// SERVER (ключевые поля + наши заголовки)
log_block($logFile, 'SERVER', [
    'REQUEST_METHOD'  => $_SERVER['REQUEST_METHOD'] ?? null,
    'SCRIPT_FILENAME' => $_SERVER['SCRIPT_FILENAME'] ?? null,
    'SCRIPT_NAME'     => $_SERVER['SCRIPT_NAME'] ?? null,
    'QUERY_STRING'    => $_SERVER['QUERY_STRING'] ?? null,
    'REMOTE_ADDR'     => $_SERVER['REMOTE_ADDR'] ?? null,
    'CONTENT_TYPE'    => $_SERVER['CONTENT_TYPE'] ?? null,
    'CONTENT_LENGTH'  => $_SERVER['CONTENT_LENGTH'] ?? null,

    // очередь/сообщение (могут быть как в $_SERVER, так и только в getenv)
    'QUEUE'           => $_SERVER['QUEUE'] ?? getenv('QUEUE') ?? null,
    'MSG_ID'          => $_SERVER['MSG_ID'] ?? getenv('MSG_ID') ?? null,
    'ATTEMPT'         => $_SERVER['ATTEMPT'] ?? getenv('ATTEMPT') ?? null,

    // idempotency key: Go кладёт как HTTP_IDEMPOTENCY_KEY
    'IDEMPOTENCY_KEY' => $_SERVER['HTTP_IDEMPOTENCY_KEY'] ?? getenv('HTTP_IDEMPOTENCY_KEY') ?? null,
]);

// полный ENV (ключевая часть теста)
log_block($logFile, 'ENV', getenv());

// имитация долгой работы
$sleepSec = (int)(getenv('SLEEP_SEC') ?: 5);
file_put_contents($logFile, "sleep_sec=$sleepSec\n", FILE_APPEND);
sleep($sleepSec);

file_put_contents($logFile, "=== JOB DONE ===\n", FILE_APPEND);

// Минимальный ответ в stdout (для твоего splitCGI это будет body)
echo json_encode([
    'ok'      => true,
    'queue'   => $queue,
    'msg_id'  => $msgId,
    'attempt' => (int)(getenv('ATTEMPT') ?: 0),
], JSON_UNESCAPED_SLASHES);

exit(0);

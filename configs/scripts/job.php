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
file_put_contents($logFile, "=== JOB DONE ===\n", FILE_APPEND);

$attempt = (int)(getenv('ATTEMPT') ?: 0);

$out = json_encode([
    'ok'      => true,
    'queue'   => $queue,
    'msg_id'  => $msgId,
    'attempt' => $attempt,
], JSON_UNESCAPED_SLASHES);

if ($attempt < 3) {
    $err = "forced fail on attempt=$attempt\n";
    file_put_contents($logFile, "=== STDOUT ===\n$out\n\n", FILE_APPEND);
    file_put_contents($logFile, "=== STDERR ===\n$err\n\n", FILE_APPEND);
    echo $out;
    fwrite(STDERR, $err);
    exit(1);
}

// success on attempt >= 3
file_put_contents($logFile, "=== STDOUT ===\n$out\n\n", FILE_APPEND);
echo $out;
exit(0);

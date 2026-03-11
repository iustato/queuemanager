<?php
$queue = getenv('QUEUE') ?: 'no_queue';
$msgId = getenv('MSG_ID') ?: 'no_msg_id';
$attempt = (int)(getenv('ATTEMPT') ?: 0);

// Fail on attempts 0 and 1, succeed on attempt 2+
if ($attempt < 2) {
    fwrite(STDERR, "attempt {$attempt}: simulated failure\n");
    echo json_encode(['ok' => false, 'attempt' => $attempt, 'error' => 'not ready yet']);
    exit(1);
}

echo json_encode([
    'ok' => true,
    'queue' => $queue,
    'msg_id' => $msgId,
    'attempt' => $attempt,
    'message' => 'succeeded after retries',
]);

exit(0);

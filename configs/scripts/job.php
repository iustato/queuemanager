<?php
$queue = getenv('QUEUE') ?: 'no_queue';
$msgId = getenv('MSG_ID') ?: 'no_msg_id';
$attempt = (int)(getenv('ATTEMPT') ?: 0);

echo json_encode([
    'ok' => true,
    'queue' => $queue,
    'msg_id' => $msgId,
    'attempt' => $attempt,
], JSON_UNESCAPED_SLASHES);

exit(0);
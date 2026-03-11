<?php
$queue = getenv('QUEUE') ?: 'no_queue';
$msgId = getenv('MSG_ID') ?: 'no_msg_id';
$attempt = (int)(getenv('ATTEMPT') ?: 0);

// Simulate framework bootstrap + autoloading (~15ms)
usleep(15000);

// Simulate CPU work: hashing, json encode/decode
$data = [];
for ($i = 0; $i < 200; $i++) {
    $data[] = hash('sha256', "row-{$i}-{$msgId}");
}
$json = json_encode($data);
$decoded = json_decode($json, true);

// Simulate DB query (~10ms)
usleep(10000);

echo json_encode([
    'ok' => true,
    'queue' => $queue,
    'msg_id' => $msgId,
    'attempt' => $attempt,
    'rows' => count($decoded),
], JSON_UNESCAPED_SLASHES);

exit(0);

<?php
header('Content-Type: text/plain');

echo "=== PHP CGI TEST ===\n\n";

echo "GET:\n";
var_export($_GET);
echo "\n\n";

echo "POST:\n";
var_export($_POST);
echo "\n\n";

echo "RAW INPUT:\n";
$raw = file_get_contents("php://input");
var_export($raw);
echo "\n\n";

echo "SERVER:\n";
var_export([
    'REQUEST_METHOD'  => $_SERVER['REQUEST_METHOD'] ?? null,
    'SCRIPT_FILENAME' => $_SERVER['SCRIPT_FILENAME'] ?? null,
    'QUERY_STRING'    => $_SERVER['QUERY_STRING'] ?? null,
    'REMOTE_ADDR'     => $_SERVER['REMOTE_ADDR'] ?? null,
    'CONTENT_TYPE'    => $_SERVER['CONTENT_TYPE'] ?? null,
    'CONTENT_LENGTH'  => $_SERVER['CONTENT_LENGTH'] ?? null,
]);
echo "\n";

<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\ConsumerSession;
use function Amp\trapSignal;

/** @var \Psr\Log\LoggerInterface $logger */
$logger = require_once __DIR__ . '/logger.php';

$client = new Client(
    new Config(
        seeds: [
            'kafka-1:9092',
            'kafka-2:9092',
            'kafka-3:9092',
        ],
    ),
    $logger,
);

$ctx = $client->consume(['events'], static function (
    array $records,
    ConsumerSession $session,
): void {
    dump($records);
    $session->commitRecords($records);
}, new Consumer\Config(group: new Consumer\GroupConfig(
    groupId: 'thesis-consumer',
)));

trapSignal([\SIGINT, \SIGTERM]);
$ctx->close();
$ctx->join();

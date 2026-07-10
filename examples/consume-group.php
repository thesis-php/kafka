<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Consumer;
use function Amp\async;
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

$consumer = $client->createConsumer(['events'], new Consumer\Config(group: new Consumer\GroupConfig(
    groupId: 'thesis-consumer',
)));

$future = async(static function () use (
    $consumer,
    $client,
): void {
    trapSignal([\SIGINT, \SIGTERM]);

    $consumer->close();
    $client->close();
});

foreach ($consumer->consume() as $batch) {
    dump($batch);
    $consumer->commitRecords($batch);
}

$future->await();

<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use function Amp\async;
use function Amp\trapSignal;

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
));

$consumer = $client->createConsumer(['events']);

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
}

$future->await();

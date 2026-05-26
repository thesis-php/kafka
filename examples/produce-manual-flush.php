<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Producer;
use function Amp\Future\awaitAll;

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
));

$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
    manualFlush: true,
));

$batch = 100;

$futures = [];
for ($i = 0; $i < $batch; ++$i) {
    $futures[] = $producer->produce(new Producer\Record("batch={$batch} record={$i}"));
}

$producer->flush();
dump(awaitAll($futures));

$producer->close();
$client->close();

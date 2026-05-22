<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\ProduceRecord;

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
    producer: new Config\ProducerConfig(
        defaultTopic: 'events',
        manualFlush: true,
    ),
));

$batch = 100;

$futures = [];
for ($i = 0; $i < $batch; ++$i) {
    $futures[] = $client->produce(new ProduceRecord("batch={$batch} record={$i}"));
}

$client->flush();
dump(\Amp\Future\awaitAll($futures));

$client->close();

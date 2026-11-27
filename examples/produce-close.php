<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
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
));

$futures = [];
for ($i = 0; $i < 10; ++$i) {
    $futures[] = $producer->produce(new Producer\Record("record={$i}"));
}

// Wait at most 5 seconds for the records. The records that are not
// delivered by then fail with Amp\CancelledException.
$producer->close(new TimeoutCancellation(5));

[$errors, $produced] = awaitAll($futures);
dump($produced, $errors);

$client->close();

<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Exception\ProduceTimedOut;
use Thesis\Kafka\Producer;
use Thesis\Time\TimeSpan;

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
    requestTimeout: TimeSpan::fromSeconds(5),
));

// Each attempt waits at most 10 + 5 seconds for the broker.
$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
    produceTimeout: TimeSpan::fromSeconds(10),
    produceAttempts: 3,
));

try {
    dump($producer->produceSync(new Producer\Record('hello')));
} catch (ProduceTimedOut $e) {
    // The broker did not answer any of the 3 attempts.
    dump("No answer for {$e->topic}-{$e->partition}");
}

$producer->close();
$client->close();

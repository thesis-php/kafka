<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Producer;

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

$future = $producer->produce(new Producer\Record('x', 'y'));
dump($future->await());

<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\ByteStream;
use Amp\Log\ConsoleFormatter;
use Amp\Log\StreamHandler;
use Monolog\Logger;
use Monolog\Processor\PsrLogMessageProcessor;
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Producer;

$logHandler = new StreamHandler(ByteStream\getStdout());
$logHandler->pushProcessor(new PsrLogMessageProcessor());
$logHandler->setFormatter(new ConsoleFormatter());
$logger = new Logger('thesis.kafka');
$logger->pushHandler($logHandler);
$logger->useLoggingLoopDetection(false);

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

$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
));

$future = $producer->produce(new Producer\Record('x', 'y'));
dump($future->await());

$producer->close();
$client->close();

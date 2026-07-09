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
use Thesis\Kafka\Consumer;
use function Amp\async;
use function Amp\trapSignal;

$logHandler = new StreamHandler(ByteStream\getStdout());
$logHandler->pushProcessor(new PsrLogMessageProcessor());
$logHandler->setFormatter(new ConsoleFormatter());
$logger = new Logger('thesis.kafka');
$logger->pushHandler($logHandler);
$logger->useLoggingLoopDetection(false);

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
), $logger);

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

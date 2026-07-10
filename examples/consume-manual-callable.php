<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\ConsumerSession;
use function Amp\trapSignal;

$client = new Client(new Config(
    seeds: [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ],
));

$ctx = $client->consume(['events'], static function (
    array $records,
    ConsumerSession $session,
): void {
    dump($records);
});

trapSignal([\SIGINT, \SIGTERM]);
$ctx->close();
$ctx->join();

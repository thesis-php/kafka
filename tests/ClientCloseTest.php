<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;
use function Amp\async;
use function Amp\delay;

final readonly class ClientCloseTest
{
    #[Test]
    public function requestAfterCloseFails(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $response = $kafka->client->request(self::apiVersionsRequest());
            Assert::same($response->errorCode, 0);

            $kafka->client->close();

            Expect::exception(ConnectionClosed::class)
                ->withMessageContaining('closed');

            $kafka->client->request(self::apiVersionsRequest());
        });
    }

    #[Test]
    public function closeDuringRequestDoesNotTryOtherBrokers(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $request = async(static fn() => $kafka->client->request(Kafka::metadataRequest()));

            // Let the request start to connect to the first seed.
            delay(0);
            $kafka->client->close();

            // Without the closed state the request goes to the next seed and gets a response.
            Expect::exception(ConnectionClosed::class)
                ->withMessage('Cluster is closed.');

            $request->await();
        });
    }

    #[Test]
    public function closedConsumersLeaveNoEventLoopWork(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $kafka->producer()->produceSync(new Producer\Record(value: 'r0', topic: $topic));

            /** @var list<Consumer\Record> $records */
            $records = [];

            $left = $kafka->loopWorkAfter(static function () use ($kafka, $topic, &$records): void {
                $direct = $kafka->directConsumer($topic, new Consumer\Config(autoOffsetReset: AutoOffsetReset::Earliest));
                $records = [...$records, ...$kafka->consume($direct, count: 1)];
                $direct->close();

                $group = $kafka->groupConsumer($topic);
                $records = [...$records, ...$kafka->consume($group, count: 1)];
                $group->close();
            });

            Assert::same($left, []);
            Assert::same(
                array_map(static fn(Consumer\Record $record) => [$record->topic, $record->partition, $record->offset, $record->value], $records),
                [[$topic, 0, 0, 'r0'], [$topic, 0, 0, 'r0']],
            );
        });
    }

    private static function apiVersionsRequest(): Generated\ApiVersionsRequest
    {
        return new Generated\ApiVersionsRequest(
            clientSoftwareName: 'thesis-kafka-tests',
            clientSoftwareVersion: 'dev',
        );
    }
}

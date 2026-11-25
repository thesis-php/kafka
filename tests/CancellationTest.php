<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\SASL\Plain\PlainAuthenticator;
use Thesis\Kafka\Testing\Kafka;
use function Amp\async;
use function Amp\delay;

final readonly class CancellationTest
{
    #[Test]
    public function requestCancelledWhileConnectingEndsWithCancellation(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            // The client has no connection yet, so the cancellation comes while it connects.
            Expect::exception(CancelledException::class);

            $kafka->client->request(Kafka::metadataRequest(), new TimeoutCancellation(0));
        });
    }

    #[Test]
    public function requestCancelledWhileWaitingForResponseEndsWithCancellation(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            // The first request learns the brokers. The second one opens a connection to one of them.
            $response = $kafka->client->request(Kafka::metadataRequest());
            Assert::count($response->brokers, 3);
            $response = $kafka->client->request(Kafka::metadataRequest());
            Assert::count($response->brokers, 3);

            // The connection is open now, so the cancellation comes while the request waits for its response.
            Expect::exception(CancelledException::class);

            $kafka->client->request(Kafka::metadataRequest(), new TimeoutCancellation(0));
        });
    }

    #[Test]
    public function cancelledCallerDoesNotFailConnectOfOtherCaller(): void
    {
        // One seed: the other caller cannot move to the next broker.
        Kafka::connect('kafka-plain-standalone:9092')
            ->sasl(new SASLConfig([
                new PlainAuthenticator(
                    username: 'test',
                    password: 'test-secret',
                ),
            ]))
            ->test(static function (Kafka $kafka): void {
                $cancellation = new DeferredCancellation();

                // Both requests wait for the same new connection.
                $cancelled = async(static fn() => $kafka->client->request(Kafka::metadataRequest(), $cancellation->getCancellation()));
                $other = async(static fn() => $kafka->client->request(Kafka::metadataRequest()));

                delay(0);
                $cancellation->cancel();

                [$errors, $responses] = Future\awaitAll([$cancelled, $other]);

                Assert::same(array_map(get_debug_type(...), $errors), [0 => CancelledException::class]);
                Assert::same(array_keys($responses), [1]);
                $response = $responses[1] ?? null;
                \assert($response instanceof Generated\MetadataResponse);
                Assert::same($response->errorCode, 0);
                Assert::count($response->brokers, 1);
            });
    }

    #[Test]
    public function closeWhileGroupConsumerJoinsEndsConsumeWithoutError(): void
    {
        $discovered = new DeferredFuture();

        // The first join after the coordinator is found loads metadata through the seeds.
        $logger = new Logger('test', [new TestHandler()], [
            static function (LogRecord $record) use ($discovered): LogRecord {
                if ($record->message === 'group coordinator discovered' && !$discovered->isComplete()) {
                    $discovered->complete();
                }

                return $record;
            },
        ]);

        Kafka::connect()->logger($logger)->test(static function (Kafka $kafka) use ($discovered): void {
            $topic = $kafka->topic();
            $consumer = $kafka->groupConsumer($topic);

            $consume = async(static function () use ($consumer): array {
                $batches = [];

                foreach ($consumer->consume(Consumer\ConsumeMode::Poll, new TimeoutCancellation(10)) as $batch) {
                    $batches[] = $batch;
                }

                return $batches;
            });

            $discovered->getFuture()->await(new TimeoutCancellation(10));

            $consumer->close();

            Assert::same($consume->await(), []);
        });
    }
}

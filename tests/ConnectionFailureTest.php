<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use Revolt\EventLoop;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\Exception\UnexpectedValue;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\BrokenPeer;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\PeerFault;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

final readonly class ConnectionFailureTest
{
    private const int CALLERS = 50;
    private const string GROUP = 'thesis-group-connection-failure';

    /**
     * The memory that a failed request may take. It is much smaller than the
     * garbage of {@see BrokenPeer}, so a client that buffers it fails the test.
     */
    private const int MEMORY_MARGIN_BYTES = 8 << 20;

    #[Test]
    public function peerThatClosesConnectionFailsEveryRequest(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::Close);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);

            /** @var array<array-key, \Throwable> $failures */
            $failures = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($client, &$failures): void {
                $requests = [];

                for ($i = 0; $i < 5; ++$i) {
                    $requests[] = async(static fn() => $client->request(self::findCoordinatorRequest()));
                }

                [$failures] = Future\awaitAll($requests, new TimeoutCancellation(10));

                // One more request opens one more connection.
                try {
                    $client->request(self::findCoordinatorRequest(), new TimeoutCancellation(10));
                } catch (\Throwable $e) {
                    $failures[] = $e;
                }
            });

            Assert::same(array_values(array_unique(self::describe($errors))), []);
            Assert::same(self::classes($failures), array_fill(0, 6, AllBrokersFailed::class), implode("\n", self::describe($failures)));
            Assert::same($peer->accepted(), 2);
        });
    }

    #[Test]
    public function closedConnectionFailsCallersThatWaitForSendQueue(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $proxy = $kafka->proxy();
            $client = $kafka->createClient(seeds: [$proxy->endpoint]);

            Assert::same(self::coordinatorKeys($client->request(self::findCoordinatorRequest())), [self::GROUP]);

            /** @var array<array-key, \Throwable> $failures */
            $failures = [];

            /** @var array<array-key, Generated\FindCoordinatorResponse> $responses */
            $responses = [];

            $errors = $kafka->loopErrorsDuring(static function () use ($client, $proxy, &$failures, &$responses): void {
                $requests = [];

                for ($i = 0; $i < self::CALLERS; ++$i) {
                    $requests[] = async(static fn() => $client->request(self::findCoordinatorRequest()));
                }

                // The callers run first and fill the send queue. Most of them wait for a free place.
                // Then the connection closes, before the writer sends their requests.
                EventLoop::queue($proxy->disconnect(...));

                [$failures, $responses] = Future\awaitAll($requests, new TimeoutCancellation(10));
            });

            Assert::same(array_values(array_unique(self::describe($errors))), []);
            Assert::count($responses, 0);
            Assert::same(self::classes($failures), array_fill(0, self::CALLERS, AllBrokersFailed::class), implode("\n", array_unique(self::describe($failures))));
        });
    }

    #[Test]
    public function silentPeerFailsRequestAfterRequestTimeoutAndLeavesNothing(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::Stall);
            $client = $kafka->createClient(
                seeds: [$peer->endpoint],
                connectTimeout: TimeSpan::fromSeconds(1),
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            $failure = null;
            $elapsed = 0.0;

            $left = $kafka->loopWorkAfter(static function () use ($client, &$failure, &$elapsed): void {
                $start = hrtime(true);

                try {
                    $client->request(self::findCoordinatorRequest());
                } catch (\Throwable $e) {
                    $failure = $e;
                }

                $elapsed = (hrtime(true) - $start) / 1e9;

                $client->close();
            });

            Assert::same($left, []);
            Assert::same(self::classes([$failure]), [CancelledException::class], implode("\n", self::describe([$failure])));
            // The request timeout is 1 s. A timer of the event loop can fire a little
            // early, so the lower limit has a margin. It still shows a real wait.
            Assert::float($elapsed)->greaterThan(0.9)->lessThan(3.0);
            Assert::same($peer->accepted(), 1);
            Assert::same($peer->openConnections(), 0);
        });
    }

    #[Test]
    public function garbageFrameLengthFailsRequestWithoutMemoryGrowth(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::Garbage);
            $client = $kafka->createClient(
                seeds: [$peer->endpoint],
                requestTimeout: TimeSpan::fromSeconds(2),
            );

            $failure = null;

            // The peer runs in this process, so the peak also counts its own buffers.
            memory_reset_peak_usage();
            $before = memory_get_usage();

            try {
                $client->request(self::findCoordinatorRequest());
            } catch (\Throwable $e) {
                $failure = $e;
            }

            $growth = memory_get_peak_usage() - $before;

            Assert::true(
                $growth < self::MEMORY_MARGIN_BYTES,
                \sprintf('Memory grew by %d bytes during the request. The peer sent %d bytes of garbage.', $growth, BrokenPeer::GARBAGE_BYTES),
            );
            Assert::same(self::classes([$failure]), [UnexpectedValue::class], implode("\n", self::describe([$failure])));
            Assert::same($peer->accepted(), 1);
            Assert::same($kafka->poll(static fn() => $peer->openConnections())->until(static fn(int $open) => $open === 0), 0);
        });
    }

    #[Test]
    public function consumerClosedWhileConnectingLeavesNoConnection(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::Stall);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);
            $consumer = $kafka->groupConsumer('thesis-it-never-created', client: $client);

            $left = $kafka->loopWorkAfter(static function () use ($kafka, $peer, $consumer): void {
                $consume = async(static function () use ($consumer): void {
                    foreach ($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(10)) as $_);
                });

                $kafka
                    ->poll(static fn() => $peer->openConnections())
                    ->expecting('a connection to the peer')
                    ->until(static fn(int $open) => $open === 1);

                $consumer->close();

                try {
                    $consume->await(new TimeoutCancellation(10));
                } catch (\Throwable) {
                    // How consume() ends after close() is not a part of this test.
                }
            });

            Assert::same($left, []);
            Assert::same($peer->accepted(), 1);
            Assert::same($peer->openConnections(), 0);
        });
    }

    #[Test]
    public function timedOutRequestDoesNotLeaveSilentConnectionInUse(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka): void {
            $proxy = $kafka->proxy();
            $client = $kafka->createClient(
                seeds: [$proxy->endpoint],
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            Assert::same(self::coordinatorKeys($client->request(self::findCoordinatorRequest())), [self::GROUP]);

            // The connection stays open, but the broker never answers on it.
            $proxy->stall();

            $failure = null;

            try {
                $client->request(self::findCoordinatorRequest());
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::same(self::classes([$failure]), [CancelledException::class], implode("\n", self::describe([$failure])));

            // The next request gets a new connection that works.
            Assert::same(self::coordinatorKeys($client->request(self::findCoordinatorRequest())), [self::GROUP]);
        });
    }

    #[Test]
    public function earlyCancellationDoesNotCloseIdleConnection(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->timeout(TimeSpan::fromSeconds(20))->test(static function (Kafka $kafka) use ($log): void {
            $proxy = $kafka->proxy();
            $client = $kafka->createClient(
                seeds: [$proxy->endpoint],
                requestTimeout: TimeSpan::fromSeconds(1),
            );

            Assert::same(self::coordinatorKeys($client->request(self::findCoordinatorRequest())), [self::GROUP]);

            // The connection stays idle for longer than the request timeout.
            delay(1.5);

            // The caller stops waiting at once, for its own reason. The broker is fine.
            $failure = null;
            $cancellation = new DeferredCancellation();
            $request = async(static fn() => $client->request(self::findCoordinatorRequest(), $cancellation->getCancellation()));
            EventLoop::queue($cancellation->cancel(...));

            try {
                $request->await();
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::same(self::classes([$failure]), [CancelledException::class], implode("\n", self::describe([$failure])));

            // The next request uses the same connection.
            Assert::same(self::coordinatorKeys($client->request(self::findCoordinatorRequest())), [self::GROUP]);
            Assert::same($proxy->accepted(), 1);
            Assert::same(
                array_values(array_map(
                    static fn(LogRecord $record) => $record->message,
                    array_filter($log->getRecords(), static fn(LogRecord $record) => $record->level === Level::Warning),
                )),
                [],
            );
        });
    }

    /**
     * @param array<array-key, ?\Throwable> $errors
     * @return list<string> class and message of each error
     */
    private static function describe(array $errors): array
    {
        return array_values(array_map(
            static fn(?\Throwable $error) => $error === null ? 'no error' : \sprintf('%s: %s', $error::class, $error->getMessage()),
            $errors,
        ));
    }

    /**
     * @param array<array-key, ?\Throwable> $errors
     * @return list<string>
     */
    private static function classes(array $errors): array
    {
        return array_values(array_map(get_debug_type(...), $errors));
    }

    /**
     * FindCoordinator goes to a seed broker and does not load metadata, so
     * the client never learns the real brokers behind a proxy.
     */
    private static function findCoordinatorRequest(): Generated\FindCoordinatorRequest
    {
        return new Generated\FindCoordinatorRequest(
            key: self::GROUP,
            keyType: 0,
            coordinatorKeys: [self::GROUP],
        );
    }

    /**
     * @return list<string>
     */
    private static function coordinatorKeys(Generated\FindCoordinatorResponse $response): array
    {
        return array_map(
            static fn(Generated\FindCoordinatorResponse\Coordinator $coordinator) => $coordinator->key,
            $response->coordinators,
        );
    }
}

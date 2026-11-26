<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Time\TimeSpan;
use function Amp\delay;

final readonly class MetadataRefreshTest
{
    #[Test]
    public function failedPeriodicRefreshIsLoggedAndTriedAgain(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->test(static function (Kafka $kafka) use ($log): void {
            $proxy = $kafka->proxy();
            $client = $kafka->createClient(
                seeds: [$proxy->endpoint],
                metadataMaxAge: TimeSpan::fromSeconds(1),
            );

            // FindCoordinator does not load metadata, so the client knows only the proxy.
            $groupId = $kafka->groupId();
            $response = $client->request(new Generated\FindCoordinatorRequest(
                key: $groupId,
                keyType: 0,
                coordinatorKeys: [$groupId],
            ));
            Assert::same(array_map(static fn($coordinator) => $coordinator->key, $response->coordinators), [$groupId]);

            $proxy->disconnect();

            $failures = $kafka
                ->poll(static fn(Cancellation $_) => array_values(array_filter(
                    $log->getRecords(),
                    static fn(LogRecord $record) => $record->level === Level::Warning && $record->message === 'periodic metadata refresh failed',
                )))
                ->expecting('two failed periodic refreshes in the log')
                ->describedBy(static fn(array $records) => \sprintf('%d failed refreshes', \count($records)))
                ->until(static fn(array $records) => \count($records) >= 2);

            foreach ($failures as $failure) {
                Assert::instanceOf($failure->context['exception'] ?? null, AllBrokersFailed::class);
            }
        });
    }

    #[Test]
    public function periodicRefreshRunsAtIntervalBelowOneSecond(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->test(static function (Kafka $kafka) use ($log): void {
            self::loseBrokerOfClientWithShortInterval($kafka);

            $kafka
                ->poll(static fn(Cancellation $_) => self::periodicFailures($log))
                ->expecting('a failed periodic refresh in the log')
                ->until(static fn(array $records) => $records !== []);

            $before = \count(self::periodicFailures($log));
            delay(1.5);
            $failures = \count(self::periodicFailures($log)) - $before;

            // The interval is 0.2 s, so about 7 refreshes fail in 1.5 s.
            // A slow failed refresh makes the period longer, so 3 is still possible.
            // A tight loop makes hundreds of records. Once per second makes 1 or 2.
            Assert::true($failures >= 3 && $failures <= 15, \sprintf('%d failed refreshes in 1.5 s', $failures));
        });
    }

    #[Test]
    public function failedPeriodicRefreshIsLoggedOnce(): void
    {
        $log = new TestHandler();

        Kafka::connect()->logger(new Logger('test', [$log]))->test(static function (Kafka $kafka) use ($log): void {
            self::loseBrokerOfClientWithShortInterval($kafka);

            $failures = $kafka
                ->poll(static fn(Cancellation $_) => self::periodicFailures($log))
                ->expecting('two failed periodic refreshes in the log')
                ->describedBy(static fn(array $records) => \sprintf('%d failed refreshes', \count($records)))
                ->until(static fn(array $records) => \count($records) >= 2);

            foreach ($failures as $failure) {
                Assert::instanceOf($failure->context['exception'] ?? null, AllBrokersFailed::class);
            }

            $errors = array_filter(
                $log->getRecords(),
                static fn(LogRecord $record) => $record->message === 'metadata refresh failed',
            );
            Assert::same(\count($errors), 0);
        });
    }

    private static function loseBrokerOfClientWithShortInterval(Kafka $kafka): void
    {
        $proxy = $kafka->proxy();
        $client = $kafka->createClient(
            seeds: [$proxy->endpoint],
            metadataMaxAge: TimeSpan::fromMilliseconds(200),
        );

        // The first request starts the timer of the periodic refresh.
        // FindCoordinator does not load metadata, so the client knows only the proxy.
        $groupId = $kafka->groupId();
        $client->request(new Generated\FindCoordinatorRequest(
            key: $groupId,
            keyType: 0,
            coordinatorKeys: [$groupId],
        ));

        $proxy->disconnect();
    }

    /**
     * @return list<LogRecord>
     */
    private static function periodicFailures(TestHandler $log): array
    {
        return array_values(array_filter(
            $log->getRecords(),
            static fn(LogRecord $record) => $record->level === Level::Warning && $record->message === 'periodic metadata refresh failed',
        ));
    }
}

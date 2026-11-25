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
}

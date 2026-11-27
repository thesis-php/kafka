<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Cancellation;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Monolog\LogRecord;

/**
 * Keeps the log of a run in memory. A test reads the produce attempts from
 * it. Give {@see logger()} to {@see Setup::logger()}.
 *
 * @internal
 */
final readonly class ProduceLog
{
    private TestHandler $handler;

    public function __construct()
    {
        $this->handler = new TestHandler();
    }

    /**
     * @param callable(LogRecord): LogRecord ...$processors they see every record before it is kept
     */
    public function logger(callable ...$processors): Logger
    {
        return new Logger('test', [$this->handler], array_values($processors));
    }

    /**
     * @return list<LogRecord>
     */
    public function records(string $message): array
    {
        return array_values(array_filter(
            $this->handler->getRecords(),
            static fn(LogRecord $record) => $record->message === $message,
        ));
    }

    /**
     * @return list<mixed> the attempt of each produce request in the log
     */
    public function attempts(): array
    {
        return array_map(
            static fn(LogRecord $record) => $record->context['attempt'] ?? null,
            $this->records('producing batch'),
        );
    }

    /**
     * Waits until the log has at least $count produce requests.
     *
     * @param positive-int $count
     */
    public function waitForAttempts(Kafka $kafka, int $count): void
    {
        $kafka
            ->poll(fn(Cancellation $_) => $this->attempts())
            ->expecting("{$count} produce attempts in the log")
            ->describedBy(static fn(array $attempts) => json_encode($attempts, JSON_THROW_ON_ERROR))
            ->until(static fn(array $attempts) => \count($attempts) >= $count);
    }
}

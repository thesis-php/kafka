<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\TimeoutCancellation;
use Ramsey\Uuid\Uuid;
use Thesis\Kafka\Client;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use function Amp\async;

/**
 * Remembers what a scenario got from the kit and releases it at the end.
 *
 * @internal
 */
final class Resources
{
    private const int STEP_TIMEOUT_SECONDS = 10;

    /** @var list<array{non-empty-string, \Closure(): void}> */
    private array $closers = [];

    /** @var array<non-empty-string, true> */
    private array $topics = [];

    private ?Cluster $probe = null;

    public function __construct(
        private readonly Client $client,
    ) {}

    /**
     * Returns one cluster for all topic checks of the scenario.
     *
     * @param \Closure(): Cluster $create
     */
    public function probe(\Closure $create): Cluster
    {
        if ($this->probe !== null) {
            return $this->probe;
        }

        $probe = $create();
        $this->addCloser('probe cluster', $probe->close(...));

        return $this->probe = $probe;
    }

    /**
     * @param non-empty-string $name
     * @param \Closure(): void $close
     */
    public function addCloser(string $name, \Closure $close): void
    {
        $this->closers[] = [$name, $close];
    }

    /**
     * @param non-empty-string $topic
     */
    public function addTopic(string $topic): void
    {
        $this->topics[$topic] = true;
    }

    /**
     * @param non-empty-string $topic
     */
    public function removeTopic(string $topic): void
    {
        unset($this->topics[$topic]);
    }

    /**
     * Releases everything, also when some steps fail.
     *
     * @return list<CleanupError>
     */
    public function release(): array
    {
        $errors = [];

        // Clients created later can depend on clients created earlier.
        foreach (array_reverse($this->closers) as [$name, $close]) {
            $error = self::step($close);

            if ($error !== null) {
                $errors[] = new CleanupError("close {$name}", $error);
            }
        }

        $this->closers = [];
        $this->probe = null;

        $topics = array_keys($this->topics);
        $this->topics = [];

        if ($topics !== []) {
            $error = self::step(fn() => $this->deleteTopics($topics));

            if ($error !== null) {
                $errors[] = new CleanupError(\sprintf('delete topics [%s]', implode(', ', $topics)), $error);
            }
        }

        $error = self::step($this->client->close(...));

        if ($error !== null) {
            $errors[] = new CleanupError('close client', $error);
        }

        return $errors;
    }

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    private function deleteTopics(array $topics): void
    {
        $response = $this->client->request(new Generated\DeleteTopicsRequest(
            topics: array_map(
                static fn(string $topic) => new Generated\DeleteTopicsRequest\DeleteTopicState(
                    name: $topic,
                    topicId: Uuid::fromString(Uuid::NIL),
                ),
                $topics,
            ),
            topicNames: $topics,
            timeoutMs: self::STEP_TIMEOUT_SECONDS * 1_000,
        ));

        $failed = [];

        foreach ($response->responses as $result) {
            // A scenario can delete its topic by itself.
            if ($result->errorCode !== ErrorCode::None->value && $result->errorCode !== ErrorCode::UnknownTopicOrPartition->value) {
                $failed[] = \sprintf('%s: %s', $result->name, ErrorCode::tryFrom($result->errorCode)->name ?? $result->errorCode);
            }
        }

        if ($failed !== []) {
            throw new KafkaRefused(\sprintf('The broker did not delete topics: %s.', implode(', ', $failed)));
        }
    }

    /**
     * @param \Closure(): void $step
     */
    private static function step(\Closure $step): ?\Throwable
    {
        try {
            // A close that never ends must not stop the other steps.
            async($step)->await(new TimeoutCancellation(self::STEP_TIMEOUT_SECONDS));

            return null;
        } catch (\Throwable $e) {
            return $e;
        }
    }
}

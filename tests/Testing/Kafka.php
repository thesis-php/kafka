<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Ramsey\Uuid\Uuid;
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\ConsumerSession;
use Thesis\Kafka\ConsumerSubscription;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Internal\ConnectionFactory;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Producer;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;

/**
 * The test kit. A test gives its scenario to {@see test()}. The scenario gets
 * this object and asks it for topics and clients. When the scenario ends, the
 * kit closes the clients and deletes the topics.
 *
 * @internal
 */
final readonly class Kafka
{
    /** @var non-empty-list<non-empty-string> */
    public const array SEEDS = [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ];
    private const int DEFAULT_WAIT_SECONDS = 10;
    private const float POLL_SECONDS = 0.5;

    /**
     * @internal Only {@see Setup} creates the kit.
     */
    public function __construct(
        public Client $client,
        private Config $config,
        private Resources $resources,
        private Cancellation $cancellation,
    ) {}

    /**
     * Runs the scenario on the main test cluster with the default settings.
     *
     * @param \Closure(self): void $scenario
     * @throws \Throwable
     */
    public static function test(\Closure $scenario): void
    {
        self::connect()->test($scenario);
    }

    /**
     * Starts the settings of a run. Without seeds the run uses the main test cluster.
     *
     * @param non-empty-string ...$seeds
     */
    public static function connect(string ...$seeds): Setup
    {
        return new Setup($seeds === [] ? self::SEEDS : array_values($seeds));
    }

    /**
     * @param ?list<string> $topics null asks for all topics
     */
    public static function metadataRequest(?array $topics = null): Generated\MetadataRequest
    {
        return new Generated\MetadataRequest(
            topics: $topics === null ? null : array_map(
                static fn(string $topic) => new Generated\MetadataRequest\MetadataRequestTopic(
                    topicId: Uuid::fromString(Uuid::NIL),
                    name: $topic,
                ),
                $topics,
            ),
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        );
    }

    /**
     * Returns a unique topic name. The kit deletes a topic with this name at
     * the end. Use it for a topic that the scenario creates by itself.
     *
     * @return non-empty-string
     */
    public function topicName(): string
    {
        $topic = \sprintf('thesis-it-%s', bin2hex(random_bytes(6)));
        $this->resources->addTopic($topic);

        return $topic;
    }

    /**
     * Creates a topic and returns when every broker sees all its partitions
     * with a leader.
     *
     * @param positive-int $partitions
     * @return non-empty-string
     */
    public function topic(int $partitions = 1, ?TimeSpan $timeout = null): string
    {
        $topic = $this->topicName();

        $response = $this->client->request(
            new Generated\CreateTopicsRequest(
                topics: [
                    new Generated\CreateTopicsRequest\CreatableTopic(
                        name: $topic,
                        numPartitions: $partitions,
                        replicationFactor: 1,
                        assignments: [],
                        configs: [],
                    ),
                ],
                timeoutMs: self::DEFAULT_WAIT_SECONDS * 1_000,
                validateOnly: false,
            ),
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );

        $errorCode = $response->topics[0]->errorCode ?? null;

        if ($errorCode !== ErrorCode::None->value) {
            throw new KafkaRefused(\sprintf(
                "The broker did not create topic '%s': %s.",
                $topic,
                $errorCode === null ? 'no result' : (ErrorCode::tryFrom($errorCode)->name ?? $errorCode),
            ));
        }

        $this->waitUntilTopicReady($topic, $partitions, $timeout);

        return $topic;
    }

    /**
     * Deletes a topic. Use it only when the deletion is a part of the scenario.
     * The kit deletes the topics of the scenario by itself.
     *
     * @param non-empty-string $topic
     */
    public function deleteTopic(string $topic): void
    {
        $response = $this->client->request(
            new Generated\DeleteTopicsRequest(
                topics: [
                    new Generated\DeleteTopicsRequest\DeleteTopicState(
                        name: $topic,
                        topicId: Uuid::fromString(Uuid::NIL),
                    ),
                ],
                topicNames: [$topic],
                timeoutMs: self::DEFAULT_WAIT_SECONDS * 1_000,
            ),
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );

        $errorCode = $response->responses[0]->errorCode ?? null;

        if ($errorCode !== ErrorCode::None->value) {
            throw new KafkaRefused(\sprintf(
                "The broker did not delete topic '%s': %s.",
                $topic,
                $errorCode === null ? 'no result' : (ErrorCode::tryFrom($errorCode)->name ?? $errorCode),
            ));
        }

        $this->resources->removeTopic($topic);
    }

    public function producer(Producer\Config $config = new Producer\Config()): Producer
    {
        $producer = $this->client->createProducer($config);
        $this->resources->addCloser('producer', $producer->close(...));

        return $producer;
    }

    /**
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     */
    public function directConsumer(string|array $topics, Consumer\Config $config = new Consumer\Config()): Consumer
    {
        if ($config->group !== null) {
            throw new \InvalidArgumentException('A direct consumer has no group. Use groupConsumer().');
        }

        $consumer = $this->client->createConsumer($topics, $config);
        $this->resources->addCloser('direct consumer', $consumer->close(...));

        return $consumer;
    }

    /**
     * Creates a group consumer that reads from the earliest offset. It has
     * short session timeouts, so a rebalance in a test is fast.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param ?non-empty-string $groupId null means a new unique group
     */
    public function groupConsumer(
        string|array $topics,
        ?string $groupId = null,
        bool $autocommit = false,
        ?TimeSpan $autocommitInterval = null,
    ): Consumer {
        $consumer = $this->client->createConsumer($topics, new Consumer\Config(
            autoOffsetReset: AutoOffsetReset::Earliest,
            group: new Consumer\GroupConfig(
                groupId: $groupId ?? $this->groupId(),
                sessionTimeout: TimeSpan::fromSeconds(6),
                heartbeatInterval: TimeSpan::fromSeconds(1),
                autocommitInterval: $autocommitInterval,
                rebalanceTimeout: TimeSpan::fromSeconds(6),
                autocommit: $autocommit,
            ),
        ));
        $this->resources->addCloser('group consumer', $consumer->close(...));

        return $consumer;
    }

    /**
     * @return non-empty-string
     */
    public function groupId(): string
    {
        return \sprintf('thesis-group-%s', bin2hex(random_bytes(6)));
    }

    /**
     * Starts {@see Client::consume()}. The kit closes the subscription at the end.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param callable(non-empty-list<Consumer\Record>, ConsumerSession): void $handler
     */
    public function subscribe(
        string|array $topics,
        callable $handler,
        Consumer\Config $config = new Consumer\Config(),
    ): ConsumerSubscription {
        $subscription = $this->client->consume($topics, $handler, $config);
        $this->resources->addCloser('subscription', static function () use ($subscription): void {
            try {
                $subscription->close();
            } catch (\Throwable) {
                // close() throws the error of the handler again. The scenario gets this error from join().
            }
        });

        return $subscription;
    }

    /**
     * Creates the internal cluster of the library with the settings of the run.
     *
     * @param list<non-empty-string> $preloadTopics
     */
    public function cluster(array $preloadTopics = []): Cluster
    {
        $cluster = new Cluster(
            new Config(
                seeds: $this->config->seeds,
                sasl: $this->config->sasl,
                preloadTopics: $preloadTopics,
            ),
            ContextLogger::null(),
        );
        $this->resources->addCloser('cluster', $cluster->close(...));

        return $cluster;
    }

    /**
     * Opens a raw connection to the first seed broker.
     */
    public function connection(): Connection
    {
        $connection = new ConnectionFactory($this->config, ContextLogger::null())->open(
            $this->config->seeds[0],
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );
        $this->resources->addCloser('connection', $connection->close(...));

        return $connection;
    }

    /**
     * Reads from the consumer until it gives at least $count records.
     * Returns all records that it read.
     *
     * @param positive-int $count
     * @return list<Consumer\Record>
     */
    public function consume(Consumer $consumer, int $count, ?TimeSpan $timeout = null): array
    {
        /** @var list<Consumer\Record> $records */
        $records = [];

        /** @var \Closure(Cancellation): list<Consumer\Record> $probe */
        $probe = static function (Cancellation $cancellation) use ($consumer, &$records): array {
            $poll = new CompositeCancellation($cancellation, new TimeoutCancellation(self::POLL_SECONDS));

            foreach ($consumer->consume(ConsumeMode::Poll, $poll) as $batch) {
                $records = [...$records, ...$batch];
            }

            return $records;
        };

        // Poll again at once: the probe itself waits for records.
        $poll = new Poll($probe, $this->cancellation, interval: 0);

        return $poll
            ->expecting(\sprintf('%d records', $count))
            ->within($timeout ?? TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS))
            ->describedBy(self::describeRecords(...))
            ->until(static fn(array $records) => \count($records) >= $count);
    }

    /**
     * Starts a wait with a time limit. The probe gets a cancellation that
     * ends with the wait. Finish the chain with {@see Poll::until()}.
     *
     * @template T
     * @param \Closure(Cancellation): T $probe
     * @return Poll<T>
     */
    public function poll(\Closure $probe): Poll
    {
        return new Poll($probe, $this->cancellation);
    }

    /**
     * Waits until every broker sees all partitions of the topic with a leader.
     * Use it for a topic that the scenario creates by itself.
     * Returns the metadata response of one broker.
     *
     * @param positive-int $partitions
     */
    public function waitUntilTopicReady(string $topic, int $partitions = 1, ?TimeSpan $timeout = null): Generated\MetadataResponse
    {
        $probe = $this->probe();

        $ask = static function (Cancellation $cancellation) use ($probe, $topic): array {
            $responses = [];

            foreach (array_keys($probe->metadata($cancellation)->brokers) as $brokerId) {
                \assert($brokerId >= 0);

                try {
                    $responses[$brokerId] = $probe->request(self::metadataRequest([$topic]), $cancellation, brokerId: $brokerId);
                } catch (CancelledException $e) {
                    throw $e;
                } catch (\Throwable $e) {
                    $responses[$brokerId] = $e->getMessage();
                }
            }

            return $responses;
        };

        $responses = $this->poll($ask)
            ->expecting(\sprintf("topic '%s' with %d partitions and a leader for each on every broker", $topic, $partitions))
            ->within($timeout ?? TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS))
            ->describedBy(static fn(array $responses) => implode(', ', array_map(
                static fn(int $brokerId, ?string $state) => \sprintf('broker %d: %s', $brokerId, $state ?? 'ready'),
                array_keys($responses),
                self::topicStates($responses, $topic, $partitions),
            )))
            ->until(static fn(array $responses) => $responses !== []
                && array_filter(self::topicStates($responses, $topic, $partitions), static fn(?string $state) => $state !== null) === []);

        $response = reset($responses);
        \assert($response instanceof Generated\MetadataResponse);

        return $response;
    }

    /**
     * @param list<Consumer\Record> $records
     */
    private static function describeRecords(array $records): string
    {
        return \sprintf(
            '%d records [%s]',
            \count($records),
            implode(', ', array_map(
                static fn(Consumer\Record $record) => \sprintf('%s-%d@%d', $record->topic, $record->partition, $record->offset),
                $records,
            )),
        );
    }

    /**
     * @param array<int, Generated\MetadataResponse|string> $responses
     * @param positive-int $partitions
     * @return array<int, ?string> null when the topic is ready on this broker
     */
    private static function topicStates(array $responses, string $topic, int $partitions): array
    {
        return array_map(
            static fn(Generated\MetadataResponse|string $response) => \is_string($response) ? $response : self::topicState($response, $topic, $partitions),
            $responses,
        );
    }

    /**
     * @param positive-int $partitions
     * @return ?non-empty-string null when the topic is ready
     */
    private static function topicState(Generated\MetadataResponse $response, string $topic, int $partitions): ?string
    {
        $metadata = null;

        foreach ($response->topics as $candidate) {
            if ($candidate->name === $topic) {
                $metadata = $candidate;
            }
        }

        if ($metadata === null) {
            return 'no topic';
        }

        if ($metadata->errorCode !== ErrorCode::None->value) {
            return ErrorCode::tryFrom($metadata->errorCode)->name ?? "error {$metadata->errorCode}";
        }

        $ready = array_filter(
            $metadata->partitions,
            static fn(Generated\MetadataResponse\MetadataResponseTopic\MetadataResponsePartition $partition) => $partition->errorCode === ErrorCode::None->value && $partition->leaderId >= 0,
        );

        if (\count($ready) < $partitions) {
            return \sprintf('%d of %d partitions with a leader', \count($ready), $partitions);
        }

        return null;
    }

    /**
     * Asks single brokers, because the public client cannot choose a broker.
     */
    private function probe(): Cluster
    {
        return $this->resources->probe(
            fn() => new Cluster($this->config, ContextLogger::null()),
        );
    }

    private function limit(TimeSpan $timeout): Cancellation
    {
        return new CompositeCancellation($this->cancellation, new TimeoutCancellation($timeout->toSeconds(3)));
    }
}

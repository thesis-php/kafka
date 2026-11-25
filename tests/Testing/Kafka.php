<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Ramsey\Uuid\Uuid;
use Revolt\EventLoop;
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
use Thesis\Kafka\Internal\NegotiatedVersions;
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

    /** @var non-empty-list<non-empty-string> */
    public const array KAFKA_4_SEEDS = [
        'kafka-4-standalone:9092',
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
        private LoggerInterface $logger = new NullLogger(),
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
     * A data provider that runs one test on the main test cluster (Kafka 3.9)
     * and on the Kafka 4.x broker. The test gets a {@see Setup} and calls
     * test() on it. The name of the data set says which broker failed.
     *
     * @return array<non-empty-string, array{Setup}>
     */
    public static function versions(): array
    {
        return [
            'Kafka 3.9' => [self::connect()],
            'Kafka 4.x' => [self::connect(...self::KAFKA_4_SEEDS)],
        ];
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
     * with a leader. Without a name the topic gets a unique name.
     *
     * @param positive-int $partitions
     * @param array<non-empty-string, string> $configs topic configs, for example max.message.bytes
     * @param ?non-empty-string $name the kit deletes this topic at the end too
     * @return non-empty-string
     */
    public function topic(
        int $partitions = 1,
        ?TimeSpan $timeout = null,
        array $configs = [],
        ?string $name = null,
    ): string {
        if ($name !== null) {
            $this->resources->addTopic($name);
        }

        $topic = $name ?? $this->topicName();

        $response = $this->client->request(
            new Generated\CreateTopicsRequest(
                topics: [
                    new Generated\CreateTopicsRequest\CreatableTopic(
                        name: $topic,
                        numPartitions: $partitions,
                        replicationFactor: 1,
                        assignments: [],
                        configs: array_map(
                            static fn(string $config, string $value) => new Generated\CreateTopicsRequest\CreatableTopic\CreatableTopicConfig($config, $value),
                            array_keys($configs),
                            $configs,
                        ),
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
     * Returns the version of the API that the client uses with a broker of
     * the run, or null when the broker does not support the API. It asks the
     * broker for its versions and negotiates them as a connection does.
     *
     * @return ?non-negative-int
     */
    public function negotiatedVersion(Generated\ApiKey $apiKey): ?int
    {
        $response = $this->client->request(
            new Generated\ApiVersionsRequest(
                clientSoftwareName: 'thesis-kafka-tests',
                clientSoftwareVersion: 'dev',
            ),
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );

        $versions = new NegotiatedVersions();
        $versions->seed(...$response->apiKeys);

        return $versions->versionOf($apiKey);
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
     * Creates one more client with other settings. Without seeds it uses the
     * seeds of the run. It logs to the logger of the run. The kit closes the
     * client at the end.
     *
     * @param list<Config\Endpoint> $seeds
     */
    public function createClient(array $seeds = [], ?TimeSpan $metadataMaxAge = null): Client
    {
        $client = new Client(
            new Config(
                seeds: $seeds === [] ? $this->config->seeds : $seeds,
                sasl: $this->config->sasl,
                metadataMaxAge: $metadataMaxAge,
            ),
            $this->logger,
        );
        $this->resources->addCloser('client', $client->close(...));

        return $client;
    }

    /**
     * Starts a TCP proxy to the first seed broker. A client that uses the
     * endpoint of the proxy loses the broker when the proxy is closed.
     * The kit closes the proxy at the end.
     */
    public function proxy(): Proxy
    {
        $proxy = new Proxy($this->config->seeds[0]);
        $this->resources->addCloser('proxy', $proxy->close(...));

        return $proxy;
    }

    /**
     * Runs the step and waits until the event loop has no work left from it.
     * Work is a callback that the step created and that keeps the loop alive:
     * an enabled and referenced timer, stream watcher or signal watcher.
     * Returns the callbacks that are left, so the result is an empty list.
     *
     * @param \Closure(): void $step
     * @return list<non-empty-string> callback types with ids, for example "Repeat#c"
     */
    public function loopWorkAfter(\Closure $step): array
    {
        $before = array_flip(EventLoop::getIdentifiers());

        $step();

        $left = static fn(): array => array_values(array_map(
            static fn(string $id) => \sprintf('%s#%s', EventLoop::getType($id)->name, $id),
            array_filter(
                EventLoop::getIdentifiers(),
                static fn(string $id) => !isset($before[$id]) && EventLoop::isEnabled($id) && EventLoop::isReferenced($id),
            ),
        ));

        return $this->poll(static fn() => $left())
            ->expecting('no event loop work left')
            ->describedBy(static fn(array $callbacks) => \sprintf('callbacks [%s]', implode(', ', $callbacks)))
            ->until(static fn(array $callbacks) => $callbacks === []);
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

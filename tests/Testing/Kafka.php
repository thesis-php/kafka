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
use Thesis\Kafka\Compressor;
use Thesis\Kafka\Config;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Kafka\Consumer\Internal\GroupConsumer\Coordinator;
use Thesis\Kafka\ConsumerSession;
use Thesis\Kafka\ConsumerSubscription;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Internal\ConnectionFactory;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\NegotiatedVersions;
use Thesis\Kafka\Producer;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;
use function Amp\async;

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

    /**
     * Every connection to this broker can go through a proxy. See {@see proxiedProducer()}.
     */
    public const string PROXIED_BROKER = 'kafka-proxied-standalone:9092';

    /**
     * The listener on port 9094 of the proxied broker advertises this port of 127.0.0.1.
     */
    private const int PROXIED_PORT = 19094;
    private const int DEFAULT_WAIT_SECONDS = 10;

    /**
     * A producer that waits for a silent broker still closes at the end of the scenario.
     */
    private const int PRODUCER_CLOSE_SECONDS = 5;

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
     * @param positive-int $replicas the replication factor, the main cluster has 3 brokers
     * @return non-empty-string
     */
    public function topic(
        int $partitions = 1,
        ?TimeSpan $timeout = null,
        array $configs = [],
        ?string $name = null,
        int $replicas = 1,
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
                        replicationFactor: $replicas,
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

    /**
     * Moves the leader of a partition to another replica of it. The topic needs
     * two replicas or more. Returns when every broker sees the new leader.
     *
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @return non-negative-int the id of the new leader
     */
    public function moveLeader(string $topic, int $partition = 0): int
    {
        $before = $this->waitUntilTopicReady($topic, $partition + 1);
        $metadata = self::partitionMetadata($before, $partition) ?? throw new KafkaRefused("No metadata of {$topic}-{$partition}.");

        $others = array_values(array_filter($metadata->replicaNodes, static fn(int $replica) => $replica !== $metadata->leaderId));
        $leader = $others[0] ?? throw new KafkaRefused("{$topic}-{$partition} has no other replica.");
        \assert($leader >= 0);

        // The first replica is the preferred leader. The order changes, the
        // replicas stay the same, so the reassignment moves no data.
        $reassignment = $this->client->request(
            new Generated\AlterPartitionReassignmentsRequest(
                timeoutMs: self::DEFAULT_WAIT_SECONDS * 1_000,
                allowReplicationFactorChange: false,
                topics: [
                    new Generated\AlterPartitionReassignmentsRequest\ReassignableTopic($topic, [
                        new Generated\AlterPartitionReassignmentsRequest\ReassignableTopic\ReassignablePartition(
                            partitionIndex: $partition,
                            replicas: [$leader, ...array_values(array_diff($metadata->replicaNodes, [$leader]))],
                        ),
                    ]),
                ],
            ),
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );

        $errors = Errors::for($reassignment);
        if (!$errors->empty()) {
            throw new KafkaRefused((string) $errors);
        }

        $probe = $this->probe();
        $client = $this->client;

        // The controller can answer before the new order is applied. Then the
        // election does not move the leader yet, so ask again. The reassignment
        // can also elect the new leader itself. Then the election is not needed.
        $this->poll(static function (Cancellation $cancellation) use ($client, $probe, $topic, $partition): array {
            $election = Errors::for($client->request(
                new Generated\ElectLeadersRequest(
                    electionType: 0, // the preferred replica
                    topicPartitions: [new Generated\ElectLeadersRequest\TopicPartitions($topic, [$partition])],
                    timeoutMs: self::DEFAULT_WAIT_SECONDS * 1_000,
                ),
                $cancellation,
            ));

            foreach ($election as $error) {
                if (!\in_array($error->code(), [ErrorCode::ElectionNotNeeded, ErrorCode::PreferredLeaderNotAvailable], true)) {
                    throw new KafkaRefused((string) $election);
                }
            }

            $leaders = [];

            foreach (array_keys($probe->metadata($cancellation)->brokers) as $brokerId) {
                \assert($brokerId >= 0);

                $response = $probe->request(self::metadataRequest([$topic]), $cancellation, brokerId: $brokerId);
                $leaders[$brokerId] = self::partitionMetadata($response, $partition)->leaderId ?? -1;
            }

            return $leaders;
        })
            ->expecting("broker {$leader} as the leader of {$topic}-{$partition} on every broker")
            ->describedBy(static fn(array $leaders) => json_encode($leaders, JSON_THROW_ON_ERROR))
            ->until(static fn(array $leaders) => $leaders !== [] && array_values(array_unique($leaders)) === [$leader]);

        return $leader;
    }

    /**
     * Deletes the records of a partition before the offset. The log of the
     * partition then starts at this offset.
     *
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param non-negative-int $offset not more than the end of the log
     */
    public function deleteRecords(string $topic, int $partition, int $offset): void
    {
        $probe = $this->probe();
        $limit = $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS));

        // Only the leader of the partition deletes records.
        $leader = $probe->lookup([$topic], $limit)->topics[$topic]->partitions[$partition]->leader ?? -1;
        if ($leader < 0) {
            throw new KafkaRefused("{$topic}-{$partition} has no leader.");
        }

        $response = $probe->request(
            new Generated\DeleteRecordsRequest(
                topics: [
                    new Generated\DeleteRecordsRequest\DeleteRecordsTopic($topic, [
                        new Generated\DeleteRecordsRequest\DeleteRecordsTopic\DeleteRecordsPartition($partition, $offset),
                    ]),
                ],
                timeoutMs: self::DEFAULT_WAIT_SECONDS * 1_000,
            ),
            $limit,
            brokerId: $leader,
        );

        $result = $response->topics[0]->partitions[0] ?? null;

        if ($result?->errorCode !== ErrorCode::None->value || $result->lowWatermark !== $offset) {
            throw new KafkaRefused(\sprintf(
                'The broker did not delete the records of %s-%d before %d: %s.',
                $topic,
                $partition,
                $offset,
                $result === null ? 'no result' : (ErrorCode::tryFrom($result->errorCode)->name ?? $result->errorCode),
            ));
        }
    }

    /**
     * Commits an offset for a group that has no members, as an admin tool
     * does. The broker does not check the offset.
     *
     * @param non-empty-string $groupId
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @param non-negative-int $offset
     */
    public function commitOffset(string $groupId, string $topic, int $partition, int $offset): void
    {
        $limit = $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS));
        $probe = $this->probe();

        $response = $this->coordinator($groupId)->request(
            new Generated\OffsetCommitRequest(
                groupId: $groupId,
                generationIdOrMemberEpoch: -1, // no generation: a commit from outside the group
                memberId: '',
                groupInstanceId: null,
                retentionTimeMs: -1,
                topics: [
                    new Generated\OffsetCommitRequest\OffsetCommitRequestTopic(
                        name: $topic,
                        topicId: $probe->lookup([$topic], $limit)->topics[$topic]->topicId ?? Uuid::fromString(Uuid::NIL),
                        partitions: [
                            new Generated\OffsetCommitRequest\OffsetCommitRequestTopic\OffsetCommitRequestPartition(
                                partitionIndex: $partition,
                                committedOffset: $offset,
                                committedLeaderEpoch: -1,
                                committedMetadata: null,
                            ),
                        ],
                    ),
                ],
            ),
            $limit,
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new KafkaRefused((string) $errors);
        }
    }

    /**
     * Returns the ids of the members of a group, as its coordinator sees them.
     *
     * @param non-empty-string $groupId
     * @return list<string>
     */
    public function groupMembers(string $groupId): array
    {
        $response = $this->coordinator($groupId)->request(
            new Generated\DescribeGroupsRequest(groups: [$groupId], includeAuthorizedOperations: false),
            $this->limit(TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS)),
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new KafkaRefused((string) $errors);
        }

        return array_map(
            static fn(Generated\DescribeGroupsResponse\DescribedGroup\DescribedGroupMember $member) => $member->memberId,
            $response->groups[0]->members ?? [],
        );
    }

    /**
     * @param ?Client $client the client of the producer, the client of the run by default
     */
    public function producer(Producer\Config $config = new Producer\Config(), ?Client $client = null): Producer
    {
        $producer = ($client ?? $this->client)->createProducer($config);
        $this->resources->addCloser('producer', static fn() => $producer->close(new TimeoutCancellation(self::PRODUCER_CLOSE_SECONDS)));

        return $producer;
    }

    /**
     * Starts a proxy to the broker {@see PROXIED_BROKER} and creates a producer
     * whose every connection goes through this proxy. Run the scenario on
     * {@see PROXIED_BROKER}.
     *
     * @param ?TimeSpan $requestTimeout the request timeout of the client of the producer
     * @return array{Proxy, Producer}
     */
    public function proxiedProducer(Producer\Config $config = new Producer\Config(), ?TimeSpan $requestTimeout = null): array
    {
        $proxy = $this->proxy(new Config\Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
        $client = $this->createClient(seeds: [$proxy->endpoint], requestTimeout: $requestTimeout);

        return [$proxy, $this->producer($config, client: $client)];
    }

    /**
     * Starts a proxy to the broker {@see PROXIED_BROKER} and creates a direct
     * consumer whose every connection goes through this proxy. Run the
     * scenario on {@see PROXIED_BROKER}.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param ?TimeSpan $requestTimeout the request timeout of the client of the consumer
     * @return array{Proxy, Consumer}
     */
    public function proxiedConsumer(string|array $topics, Consumer\Config $config = new Consumer\Config(), ?TimeSpan $requestTimeout = null): array
    {
        $proxy = $this->proxy(new Config\Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
        $client = $this->createClient(seeds: [$proxy->endpoint], requestTimeout: $requestTimeout);

        return [$proxy, $this->directConsumer($topics, $config, client: $client)];
    }

    /**
     * Starts a proxy to the broker {@see PROXIED_BROKER} and creates a group
     * consumer whose every connection goes through this proxy, also the one
     * to the group coordinator. Run the scenario on {@see PROXIED_BROKER}.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param ?TimeSpan $requestTimeout the request timeout of the client of the consumer
     * @return array{Proxy, Consumer}
     */
    public function proxiedGroupConsumer(
        string|array $topics,
        ?TimeSpan $requestTimeout = null,
        ?TimeSpan $sessionTimeout = null,
        ?TimeSpan $heartbeatInterval = null,
    ): array {
        $proxy = $this->proxy(new Config\Endpoint('kafka-proxied-standalone', 9094), self::PROXIED_PORT);
        $client = $this->createClient(seeds: [$proxy->endpoint], requestTimeout: $requestTimeout);

        return [$proxy, $this->groupConsumer(
            $topics,
            client: $client,
            sessionTimeout: $sessionTimeout,
            heartbeatInterval: $heartbeatInterval,
        )];
    }

    /**
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param ?Client $client the client of the consumer, the client of the run by default
     */
    public function directConsumer(string|array $topics, Consumer\Config $config = new Consumer\Config(), ?Client $client = null): Consumer
    {
        if ($config->group !== null) {
            throw new \InvalidArgumentException('A direct consumer has no group. Use groupConsumer().');
        }

        $consumer = ($client ?? $this->client)->createConsumer($topics, $config);
        $this->resources->addCloser('direct consumer', $consumer->close(...));

        return $consumer;
    }

    /**
     * Creates a group consumer that reads from the earliest offset by default. It has
     * short session timeouts, so a rebalance in a test is fast.
     *
     * @param non-empty-string|non-empty-list<non-empty-string> $topics
     * @param ?non-empty-string $groupId null means a new unique group
     * @param ?Client $client the client of the consumer, the client of the run by default
     */
    public function groupConsumer(
        string|array $topics,
        ?string $groupId = null,
        bool $autocommit = false,
        ?TimeSpan $autocommitInterval = null,
        ?Client $client = null,
        ?TimeSpan $rebalanceTimeout = null,
        ?TimeSpan $sessionTimeout = null,
        ?TimeSpan $heartbeatInterval = null,
        ?TimeSpan $maxWait = null,
        AutoOffsetReset $autoOffsetReset = AutoOffsetReset::Earliest,
        ?Compressor\Registry $decompressors = null,
    ): Consumer {
        $consumer = ($client ?? $this->client)->createConsumer($topics, $this->groupConfig(
            groupId: $groupId,
            autocommit: $autocommit,
            autocommitInterval: $autocommitInterval,
            rebalanceTimeout: $rebalanceTimeout,
            sessionTimeout: $sessionTimeout,
            heartbeatInterval: $heartbeatInterval,
            maxWait: $maxWait,
            autoOffsetReset: $autoOffsetReset,
            decompressors: $decompressors,
        ));
        $this->resources->addCloser('group consumer', $consumer->close(...));

        return $consumer;
    }

    /**
     * Returns the consumer config of {@see groupConsumer()}: the earliest
     * offset by default and short session timeouts. Use it for {@see subscribe()} with a group.
     *
     * @param ?non-empty-string $groupId null means a new unique group
     * @param ?TimeSpan $rebalanceTimeout 6 seconds by default
     * @param ?TimeSpan $sessionTimeout 6 seconds by default, the minimum of the main cluster
     * @param ?TimeSpan $heartbeatInterval 1 second by default
     * @param ?TimeSpan $maxWait how long a fetch waits for records, the default of the consumer config
     * @param AutoOffsetReset $autoOffsetReset where to start without a committed offset, and after an offset out of range
     * @param ?Compressor\Registry $decompressors the codecs that the consumer can read, all available by default
     */
    public function groupConfig(
        ?string $groupId = null,
        bool $autocommit = false,
        ?TimeSpan $autocommitInterval = null,
        ?TimeSpan $rebalanceTimeout = null,
        ?TimeSpan $sessionTimeout = null,
        ?TimeSpan $heartbeatInterval = null,
        ?TimeSpan $maxWait = null,
        AutoOffsetReset $autoOffsetReset = AutoOffsetReset::Earliest,
        ?Compressor\Registry $decompressors = null,
    ): Consumer\Config {
        return new Consumer\Config(
            decompressors: $decompressors,
            maxWait: $maxWait,
            autoOffsetReset: $autoOffsetReset,
            group: new Consumer\GroupConfig(
                groupId: $groupId ?? $this->groupId(),
                sessionTimeout: $sessionTimeout ?? TimeSpan::fromSeconds(6),
                heartbeatInterval: $heartbeatInterval ?? TimeSpan::fromSeconds(1),
                autocommitInterval: $autocommitInterval,
                rebalanceTimeout: $rebalanceTimeout ?? TimeSpan::fromSeconds(6),
                autocommit: $autocommit,
            ),
        );
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
    public function createClient(
        array $seeds = [],
        ?TimeSpan $metadataMaxAge = null,
        ?TimeSpan $connectTimeout = null,
        ?TimeSpan $requestTimeout = null,
    ): Client {
        $client = new Client(
            new Config(
                seeds: $seeds === [] ? $this->config->seeds : $seeds,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                sasl: $this->config->sasl,
                metadataMaxAge: $metadataMaxAge,
            ),
            $this->logger,
        );
        $this->resources->addCloser('client', $client->close(...));

        return $client;
    }

    /**
     * Starts a TCP proxy to the first seed broker, or to the upstream. A client
     * that uses the endpoint of the proxy loses the broker when the proxy is
     * closed. The kit closes the proxy at the end.
     *
     * @param ?Config\Endpoint $upstream the first seed broker by default
     * @param int<0, 65535> $port the local port of the proxy, 0 is a free port
     */
    public function proxy(?Config\Endpoint $upstream = null, int $port = 0): Proxy
    {
        $proxy = new Proxy($upstream ?? $this->config->seeds[0], $port);
        $this->resources->addCloser('proxy', $proxy->close(...));

        return $proxy;
    }

    /**
     * Starts a TCP server that accepts connections and then fails as the
     * fault says. It does not speak the Kafka protocol. The kit closes the
     * server at the end.
     */
    public function brokenPeer(PeerFault $fault): BrokenPeer
    {
        $peer = new BrokenPeer($fault);
        $this->resources->addCloser('broken peer', $peer->close(...));

        return $peer;
    }

    /**
     * Runs the step and returns the errors that reached the event loop while
     * it ran: exceptions that no fiber caught. The result is an empty list.
     *
     * @param \Closure(): void $step
     * @return list<\Throwable>
     */
    public function loopErrorsDuring(\Closure $step): array
    {
        $errors = [];
        $previous = EventLoop::getErrorHandler();

        EventLoop::setErrorHandler(static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        try {
            $step();
        } finally {
            EventLoop::setErrorHandler($previous);
        }

        return $errors;
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
        $probe = static function (Cancellation $cancellation) use ($consumer, $count, &$records): array {
            // The read stops after the batch that gives enough records. The other
            // batches stay in the buffer of the consumer for the next read.
            $read = async(static function () use ($consumer, $count, $cancellation, &$records): void {
                foreach ($consumer->consume(ConsumeMode::Stream, $cancellation) as $batch) {
                    $records = [...$records, ...$batch];

                    if (\count($records) >= $count) {
                        break;
                    }
                }
            });

            // The read can fail after the wait has ended. Nobody waits for it then.
            $read->ignore()->await($cancellation);

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
     * The broker does not sort the partitions of a topic by index.
     */
    private static function partitionMetadata(
        Generated\MetadataResponse $response,
        int $partition,
    ): ?Generated\MetadataResponse\MetadataResponseTopic\MetadataResponsePartition {
        foreach ($response->topics[0]->partitions ?? [] as $candidate) {
            if ($candidate->partitionIndex === $partition) {
                return $candidate;
            }
        }

        return null;
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

    /**
     * @param non-empty-string $groupId
     */
    private function coordinator(string $groupId): Coordinator
    {
        return new Coordinator(
            groupId: $groupId,
            cluster: $this->probe(),
            logger: ContextLogger::null(),
            requestTimeout: TimeSpan::fromSeconds(self::DEFAULT_WAIT_SECONDS),
        );
    }

    private function limit(TimeSpan $timeout): Cancellation
    {
        return new CompositeCancellation($this->cancellation, new TimeoutCancellation($timeout->toSeconds(3)));
    }
}

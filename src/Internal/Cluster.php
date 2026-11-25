<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use Amp\NullCancellation;
use Amp\TimeoutCancellation;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Revolt\EventLoop;
use Thesis\Kafka\Config;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\AllBrokersFailed;
use Thesis\Kafka\Exception\ConnectionClosed;
use Thesis\Kafka\Exception\NoAvailableController;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\UnknownBroker;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Internal\Cluster\Metadata;
use Thesis\Kafka\NetworkException;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use function Amp\async;

/**
 * @internal
 */
final class Cluster implements MetadataProvider
{
    private readonly ConnectionFactory $factory;

    /** @var non-empty-list<Cluster\Broker> */
    private readonly array $seeds;

    /** @var array<int, Cluster\Broker> */
    private array $brokers = [];

    /**
     * The current controller's broker ID, or -1 when the cluster has
     * no elected controller right now (mid-election). Kept in sync
     * with the last MetadataResponse.
     */
    private int $controllerId = -1;

    private ?Cluster\Metadata $metadata = null;

    private ?string $metadataCallbackId = null;

    private bool $closed = false;

    public function __construct(
        private readonly Config $config,
        private readonly ContextLogger $logger,
    ) {
        $this->factory = $factory = new ConnectionFactory(
            $config,
            // @infection-ignore-all
            $this->logger->with([
                'component' => 'connection.factory',
            ]),
        );

        $this->seeds = array_map(
            static fn(int $index, Config\Endpoint $endpoint) => new Cluster\Broker(
                Cluster\NodeId::unknown($index),
                $endpoint,
                $factory,
                $config->connectTimeout->add($config->requestTimeout),
            ),
            array_keys($this->config->seeds),
            $this->config->seeds,
        );
    }

    #[\Override]
    public function topic(
        string $topic,
        Cancellation $cancellation = new NullCancellation(),
    ): Metadata\TopicMetadata {
        return $this->metadata($cancellation)->topics[$topic]
            ?? $this->refreshMetadata([$topic], $cancellation)->topics[$topic]
            ?? throw new UnknownTopic($topic);
    }

    public function metadata(Cancellation $cancellation = new NullCancellation()): Cluster\Metadata
    {
        return $this->metadata ??= $this->refreshMetadata(cancellation: $cancellation);
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @param ?non-negative-int $brokerId
     * @param ?Retry\Operation<T> $retry
     * @return ($reply is Connection\Reply::Expected ? T : null)
     * @throws Exception
     * @throws CancelledException
     */
    public function request(// @phpstan-ignore return.unusedType
        Generated\Request $request,
        ?Cancellation $cancellation = null,
        Connection\Reply $reply = Connection\Reply::Expected,
        ?int $brokerId = null,
        ?Retry\Operation $retry = null,
    ): mixed {
        if ($reply === Connection\Reply::None && $retry !== null) {
            throw new \LogicException('Retry cannot be set with Reply::None.');
        }

        $this->ensureOpen();

        $this->metadataCallbackId ??= EventLoop::repeat(
            $this->config->metadataMaxAge->toSeconds(),
            function (): void {
                // An exception must not leave the timer callback: it stops the event loop.
                // The next tick tries again.
                try {
                    $this->refreshMetadata();
                } catch (\Throwable $e) {
                    // @infection-ignore-all
                    $this->logger->warning('periodic metadata refresh failed', [
                        'exception' => $e,
                    ]);
                }
            },
        );

        // Default to a client-side timeout based on Config: without it,
        // a slow broker would stall the caller indefinitely. Callers
        // who want different semantics pass their own Cancellation.
        $cancellation ??= new TimeoutCancellation($this->config->requestTimeout->toSeconds(3));

        /** @var \Closure(): T $call */
        $call = fn() => $this->doRequest(
            request: $request,
            cancellation: $cancellation,
            reply: $reply,
            brokerId: $brokerId,
        );

        if ($retry !== null) {
            $call = $retry->do($call, $cancellation);
        }

        return $call();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        $futures = [];

        foreach ($this->brokers as $id => $broker) {
            $futures[] = async($broker->close(...));
            unset($this->brokers[$id]);
        }

        foreach ($this->seeds as $seed) {
            $futures[] = async($seed->close(...));
        }

        if ($this->metadataCallbackId !== null) {
            EventLoop::cancel($this->metadataCallbackId);
            $this->metadataCallbackId = null;
        }

        Future\awaitAll($futures);
    }

    /**
     * @param ?list<string> $topics
     */
    public function refreshMetadata(?array $topics = null, ?Cancellation $cancellation = null): Cluster\Metadata
    {
        /** @var ?UuidInterface $nilUuid */
        static $nilUuid;
        $nilUuid ??= Uuid::fromString(Uuid::NIL);

        $request = new Generated\MetadataRequest(
            topics: array_map(
                static fn(string $topic) => new Generated\MetadataRequest\MetadataRequestTopic(
                    topicId: $nilUuid,
                    name: $topic,
                ),
                array_values(
                    array_unique(
                        array_merge($this->config->preloadTopics, $topics ?? []),
                    ),
                ),
            ),
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        );

        $this->doMetadataRequest($request, $cancellation);

        return $this->metadata;
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @param ?non-negative-int $brokerId
     * @return ($reply is Connection\Reply::Expected ? T : null)
     * @throws Exception
     * @throws CancelledException
     */
    private function doRequest(
        Generated\Request $request,
        ?Cancellation $cancellation = null,
        Connection\Reply $reply = Connection\Reply::Expected,
        ?int $brokerId = null,
    ): mixed {
        $cancellation ??= new TimeoutCancellation($this->config->requestTimeout->toSeconds(3));

        if ($brokerId !== null) {
            $broker = $this->brokers[$brokerId] ?? throw new UnknownBroker($brokerId);
            return $broker->request($request, $cancellation, $reply);
        }

        // Metadata is special: it both feeds and uses the broker set,
        // so it bypasses the normal routing and goes through the
        // dedicated refresh path below.
        if ($request instanceof Generated\MetadataRequest) {
            return $this->doMetadataRequest($request, $cancellation); // @phpstan-ignore return.type
        }

        if ($request->apiKey()->requiresAnyListener(Generated\Listener::Controller)) {
            return $this->doControllerRequest($request, $cancellation);
        }

        return $this->doBrokerRequest($request, $cancellation, $reply);
    }

    /**
     * @throws Exception
     * @phpstan-assert !null $this->metadata
     */
    private function doMetadataRequest(
        Generated\MetadataRequest $request,
        ?Cancellation $cancellation = null,
    ): Generated\MetadataResponse {
        try {
            /** @var Generated\MetadataResponse $response */
            $response = $this->doBrokerRequest($request, $cancellation);
        } catch (\Throwable $e) {
            // @infection-ignore-all
            $this->logger->error('metadata refresh failed', [
                'exception' => $e,
            ]);

            throw $e;
        }

        // Most MetadataResponse error codes are per-topic or
        // per-partition and don't invalidate the response as a
        // whole — a missing leader for one partition doesn't make
        // the broker list wrong. We only bail out on a top-level
        // error code. per-topic and per-partition errors stay in
        // the response for the caller (Producer/Consumer) to react to.
        if ($response->errorCode !== 0) {
            throw new ProtocolError(Errors::for($response));
        }

        $this->discover($response);
        $this->createMetadataSnapshot($response);

        return $response;
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws Exception
     */
    private function doControllerRequest(
        Generated\Request $request,
        ?Cancellation $cancellation = null,
    ): Generated\Response {
        $cancellation ??= new TimeoutCancellation($this->config->requestTimeout->toSeconds(3));

        // Only two attempts: first with whatever controllerId we currently
        // believe, second after a forced metadata refresh. Anything
        // beyond two attempts in a tight loop suggests a real cluster
        // issue (no controller elected, repeated split-brain) and is
        // not something blind retry can fix.
        for ($attempt = 0; $attempt < 2; ++$attempt) {
            if ($this->controllerId === -1) {
                $this->doMetadataRequest(
                    new Generated\MetadataRequest(
                        topics: [],
                        allowAutoTopicCreation: false,
                        includeClusterAuthorizedOperations: false,
                        includeTopicAuthorizedOperations: false,
                    ),
                    $cancellation,
                );
            }

            if ($this->controllerId === -1) {
                // @infection-ignore-all
                $this->logger->warning('no controller elected', [
                    'error' => ErrorCode::NotController->name,
                ]);

                // Refresh completed but cluster has no controller right now (mid-election).
                throw new NoAvailableController('No controller elected: maybe cluster is in mid-election?');
            }

            $controller = $this->brokers[$this->controllerId] ?? null;
            if ($controller === null) {
                $this->controllerId = -1;
                continue;
            }

            $response = $controller->request($request, $cancellation);

            // NOT_CONTROLLER means the broker we picked thinks it isn't
            // the controller anymore. Could be a re-election between
            // our last metadata refresh and now. Force a refresh and
            // retry again.
            if (Errors::for($response)->has(ErrorCode::NotController)) {
                $this->controllerId = -1;
                continue;
            }

            return $response;
        }

        // @infection-ignore-all
        $this->logger->warning('controller unavailable, retries exhausted', [
            'error' => ErrorCode::NotController->name,
        ]);

        throw new NoAvailableController('Controller keeps changing, exhausted retries.');
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return ($reply is Connection\Reply::Expected ? T : null)
     * @throws Exception
     */
    private function doBrokerRequest(
        Generated\Request $request,
        ?Cancellation $cancellation = null,
        Connection\Reply $reply = Connection\Reply::Expected,
    ): mixed {
        $cancellation ??= new TimeoutCancellation($this->config->requestTimeout->toSeconds(3));

        $errors = [];

        foreach ($this->candidates() as $broker) {
            try {
                return $broker->request($request, $cancellation, $reply);
            } catch (NetworkException $e) {
                // Only network errors move the request to the next broker. A cancellation
                // or an error of the request itself goes to the caller at once.
                $errors[(string) $broker->endpoint] = $e;

                // After close() no other broker is tried.
                $this->ensureOpen();
            }
        }

        throw new AllBrokersFailed($errors);
    }

    /**
     * Yields the brokers to try, in order. It is lazy: the list of cold
     * brokers is built only after all wired brokers have failed.
     *
     * @return \Generator<Cluster\Broker>
     */
    private function candidates(): \Generator
    {
        // Prefer brokers that already have a live connection. Reusing
        // a wired broker avoids TCP/TLS/ApiVersions/SASL handshakes.
        // Random pick spreads load when several are wired.
        $wired = array_filter(
            $this->brokers,
            static fn(Cluster\Broker $broker) => $broker->alive(),
        );

        while ($wired !== []) {
            $key = array_rand($wired);
            yield $wired[$key];
            unset($wired[$key]);
        }

        // Discovered brokers without an open connection. We dial
        // lazily, in random order so that all clients don't dogpile the
        // same broker when the discovered set is freshly populated.
        $cold = array_filter(
            $this->brokers,
            static fn(Cluster\Broker $broker) => !$broker->alive(),
        );

        while ($cold !== []) {
            $key = array_rand($cold);
            yield $cold[$key];
            unset($cold[$key]);
        }

        // Reached on the first bootstrap (no discovered yet) or when
        // every discovered endpoint is unreachable. The user picked
        // the seed order intentionally, so we don't randomize.
        yield from $this->seeds;
    }

    /**
     * @throws ConnectionClosed
     */
    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ConnectionClosed('Cluster is closed.');
        }
    }

    private function discover(Generated\MetadataResponse $metadata): void
    {
        $next = [];
        foreach ($metadata->brokers as $broker) {
            $next[$broker->nodeId] = Config\Endpoint::fromString("{$broker->host}:{$broker->port}");
        }

        foreach ($this->brokers as $id => $broker) {
            if (!isset($next[$id])) {
                // Do not propagate the caller's cancellation.
                // If a user passed a 100ms timeout for their Metadata request,
                // that timeout shouldn't be able to abort our broker cleanup
                // midway and leave sockets dangling.
                $broker->close();
                unset($this->brokers[$id]);
            }
        }

        foreach ($next as $id => $endpoint) {
            $broker = $this->brokers[$id] ?? null;

            // Same broker, same endpoint. Keep the
            // existing Broker object and its live connections.
            if ($broker?->endpoint->matches($endpoint) === true) {
                continue;
            }

            // Endpoint changed for an existing nodeId, close and replace.
            $broker?->close();

            $this->brokers[$id] = new Cluster\Broker(
                nodeId: new Cluster\NodeId($id),
                endpoint: $endpoint,
                factory: $this->factory,
                connectTimeout: $this->config->connectTimeout->add($this->config->requestTimeout),
            );
        }

        // Always write back what the broker reported, including -1
        // (no controller right now, mid-election). Keeping a stale
        // controllerId here would route controller-only requests to
        // the wrong broker, which would just answer NOT_CONTROLLER.
        $this->controllerId = $metadata->controllerId;
    }

    private function createMetadataSnapshot(Generated\MetadataResponse $metadata): void
    {
        $brokers = [];
        foreach ($metadata->brokers as $broker) {
            $brokers[$broker->nodeId] = new Cluster\Metadata\BrokerMetadata(
                id: $broker->nodeId,
                endpoint: Config\Endpoint::fromString("{$broker->host}:{$broker->port}"),
                rack: $broker->rack,
            );
        }

        $topics = [];
        foreach ($metadata->topics as $topic) {
            $topicName = $topic->name;
            \assert(\is_string($topicName) && $topicName !== '');

            if ($topic->errorCode !== 0) {
                $error = ErrorCode::from($topic->errorCode);
                // @infection-ignore-all
                $this->logger->warning('topic metadata error', [
                    'topic' => $topicName,
                    'errorCode' => $topic->errorCode,
                    'error' => $error->name,
                ]);
                // Topic-level error (UNKNOWN_TOPIC_OR_PARTITION, TOPIC_AUTHORIZATION_FAILED): drop from cache.
                $topics[$topicName] = null;
            } else {
                $partitions = [];

                foreach ($topic->partitions as $partition) {
                    if ($partition->leaderId < 0) {
                        // @infection-ignore-all
                        $this->logger->warning('partition has no leader', [
                            'topic' => $topicName,
                            'partition' => $partition->partitionIndex,
                        ]);
                    }

                    $partitions[$partition->partitionIndex] = new Cluster\Metadata\PartitionMetadata(
                        partition: $partition->partitionIndex,
                        leader: $partition->leaderId,
                        leaderEpoch: $partition->leaderEpoch,
                        replicas: $partition->replicaNodes,
                        isr: $partition->isrNodes,
                    );
                }

                $topics[$topicName] = new Cluster\Metadata\TopicMetadata(
                    name: $topicName,
                    topicId: $topic->topicId,
                    kind: $topic->isInternal ? Cluster\Metadata\TopicKind::Internal : Cluster\Metadata\TopicKind::User,
                    partitions: $partitions,
                );
            }
        }

        // Merge with the existing snapshot: topics that weren't requested
        // in this refresh aren't in $response, so we keep their old entries
        // rather than dropping them. Drop only topic with errorCode !== 0.
        $merged = $this->metadata->topics ?? [];

        foreach ($topics as $name => $topic) {
            if ($topic === null) {
                unset($merged[$name]);
            } else {
                $merged[$name] = $topic;
            }
        }

        $this->metadata = new Cluster\Metadata(
            brokers: $brokers,
            controllerId: $metadata->controllerId,
            topics: $merged,
            loadedAtMs: (int) (microtime(true) * 1000),
        );
    }
}

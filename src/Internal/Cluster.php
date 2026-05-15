<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\TimeoutCancellation;
use Thesis\Kafka\Config;
use Thesis\Kafka\Exception\AllSeedsFailed;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;
use function Amp\async;

/**
 * @internal
 */
final readonly class Cluster
{
    private Cluster\BrokerList $brokers;

    private Cluster\MetadataList $metadata;

    private ConnectionFactory $connections;

    public function __construct(private Config $config)
    {
        $this->brokers = new Cluster\BrokerList();
        $this->connections = new ConnectionFactory($config);
        $this->metadata = new Cluster\MetadataList(controllerId: Cluster\Broker::NOT_A_BROKER_ID);
    }

    /**
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @return T
     * @throws KafkaException
     */
    public function request(Generated\Request $request, ?Cancellation $cancellation = null): Generated\Response
    {
        $cancellation ??= new TimeoutCancellation($this->config->requestTimeout->toSeconds());

        $brokerId = $this->brokers->random();
        if ($request->apiKey()->requiresAnyListener(Generated\Listener::Controller)) {
            $brokerId = $this->metadata->controllerId;
        }

        $broker = $this->brokers->seeds[$brokerId] ?? $this->newBroker(cancellation: $cancellation);

        if ($request->apiKey()->requiresAnyListener(Generated\Listener::Controller)) {
            $broker = $this->newController($broker, $cancellation);
        }

        return $broker->request($request, $cancellation);
    }

    public function close(Cancellation $cancellation = new NullCancellation()): void
    {
        $futures = array_map(
            static fn(Cluster\Broker $broker) => async($broker->connection->close(...)),
            $this->brokers->purge(),
        );

        Future\awaitAll($futures, $cancellation);
    }

    /**
     * @throws KafkaException
     */
    private function newBroker(Cancellation $cancellation = new NullCancellation()): Cluster\Broker
    {
        $broker = $this->connectToBroker(
            $this->config->seeds,
            $cancellation,
        );

        if (\count($this->metadata) === 0) {
            $this->refreshMetadata($broker, $cancellation);
        }

        // Try to reconcile the bootstrap connection with advertised cluster metadata.
        // If the connected seed endpoint matches an advertised broker endpoint, we can
        // replace the temporary placeholder entry with the broker's real node ID.
        foreach ($this->metadata as $node) {
            if ($broker->connection->endpoint->matches($node->endpoint)) {
                $broker = new Cluster\Broker(
                    id: $node->id,
                    connection: $broker->connection,
                );

                $this->brokers->add($broker);

                return $broker;
            }
        }

        // If the bootstrap connection cannot be matched to any advertised broker
        // endpoint, treat it as a temporary bootstrap-only connection, close it,
        // and reconnect using advertised metadata so the broker pool is keyed by
        // stable node IDs.
        $broker->connection->close();

        $reasons = [];

        foreach ($this->metadata as $node) {
            try {
                $broker = $this->connectToBroker(
                    [$node->endpoint],
                    $cancellation,
                );

                return new Cluster\Broker(
                    id: $node->id,
                    connection: $broker->connection,
                );
            } catch (\Throwable $e) {
                $reasons[(string) $node->endpoint] = $e;
            }
        }

        throw new AllSeedsFailed($reasons); // TODO: change to seed and known brokers map
    }

    /**
     * @throws KafkaException
     */
    private function newController(Cluster\Broker $broker, Cancellation $cancellation): Cluster\Broker
    {
        if ($broker->id === $this->metadata->controllerId) {
            return $broker;
        }

        $controller = $this->brokers->seeds[$this->metadata->controllerId] ?? null;
        if ($controller !== null) {
            return $controller;
        }

        $node = $this->metadata->nodes[$this->metadata->controllerId] ?? null;
        if ($node === null) {
            $this->refreshMetadata($broker, $cancellation);
            $node = $this->metadata->nodes[$this->metadata->controllerId] ?? throw new \RuntimeException();
        }

        $controller = $this->connectToBroker([$node->endpoint], $cancellation);
        $controller = new Cluster\Broker(
            id: $node->id,
            connection: $controller->connection,
        );

        $this->brokers->add($controller);

        return $controller;
    }

    /**
     * @throws KafkaException
     */
    private function refreshMetadata(
        Cluster\Broker $broker,
        Cancellation $cancellation,
    ): void {
        $metadata = $broker->request(
            new Generated\MetadataRequest(
                topics: null,
                allowAutoTopicCreation: false,
                includeClusterAuthorizedOperations: true,
                includeTopicAuthorizedOperations: true,
            ),
            $cancellation,
        );

        $this->metadata->refresh($metadata);
    }

    /**
     * @param non-empty-list<Config\Endpoint> $endpoints
     * @throws KafkaException
     */
    private function connectToBroker(
        array $endpoints,
        Cancellation $cancellation,
    ): Cluster\Broker {
        $reasons = [];

        foreach ($endpoints as $endpoint) {
            try {
                $connection = $this->connections->open($endpoint, $cancellation);

                // This is a temporary bootstrap broker entry. At this point we only have
                // a live connection to one seed endpoint, but we do not yet know which
                // broker ID it corresponds to. A subsequent MetadataRequest will either
                // match this connection to a real broker ID or cause us to discard it in
                // favor of a new connection to an advertised broker endpoint.
                return Cluster\Broker::unknown(
                    $connection,
                );
            } catch (\Throwable $e) {
                $reasons[(string) $endpoint] = $e;
            }
        }

        throw new AllSeedsFailed($reasons);
    }
}

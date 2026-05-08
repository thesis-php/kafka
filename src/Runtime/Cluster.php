<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Amp\Cancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Socket;
use Amp\TimeoutCancellation;
use Thesis\Kafka\Config;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Runtime\Connection\Bootstrapper;
use function Amp\async;

/**
 * @internal
 */
final readonly class Cluster
{
    private Cluster\BrokerList $brokers;

    private Cluster\MetadataList $metadata;

    private Cluster\NegotiatedVersions $versions;

    private Bootstrapper $bootstrapper;

    public function __construct(
        private Config $config,
        ?Socket\SocketConnector $connector = null,
    ) {
        $this->brokers = new Cluster\BrokerList();
        $this->versions = new Cluster\NegotiatedVersions();
        $this->bootstrapper = new Bootstrapper($connector);
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
        $requireApiVersionsRequest = false;

        if (\count($this->versions) === 0) {
            // No cluster-wide version negotiation exists yet, so we bootstrap with the
            // only ApiVersions version we can safely assume before negotiation: v0.
            // Once the broker replies, the full response will reseed the negotiated
            // versions map, including ApiVersions itself, with the actual compatible
            // version range for this cluster.
            $this->versions->seed(new Generated\ApiVersionsResponse\ApiVersion(
                apiKey: Generated\ApiKey::ApiVersions->value,
                minVersion: 0,
                maxVersion: 0,
            ));

            $requireApiVersionsRequest = true;
        }

        $broker = $this->connectToBroker(
            $this->config->seeds,
            $cancellation,
        );

        if ($requireApiVersionsRequest) {
            $versions = $broker->request(
                new Generated\ApiVersionsRequest(
                    clientSoftwareName: $this->config->clientSoftwareName,
                    clientSoftwareVersion: $this->config->clientSoftwareVersion,
                ),
                $cancellation,
            );

            $this->versions->seed(...$versions->apiKeys);
        }

        if (\count($this->metadata) === 0) {
            $this->refreshMetadata($broker, $cancellation);
        }

        // Try to reconcile the bootstrap connection with advertised cluster metadata.
        // If the connected seed endpoint matches an advertised broker endpoint, we can
        // replace the temporary placeholder entry with the broker's real node ID.
        foreach ($this->metadata as $node) {
            if ($broker->endpoint->matches($node->endpoint)) {
                $broker = new Cluster\Broker(
                    id: $node->id,
                    connection: $broker->connection,
                    endpoint: $node->endpoint,
                    versions: $this->versions,
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

        foreach ($this->metadata as $node) {
            $broker = $this->connectToBroker([$node->endpoint], $cancellation);
            $broker = new Cluster\Broker(
                id: $node->id,
                connection: $broker->connection,
                endpoint: $node->endpoint,
                versions: $this->versions,
            );
        }

        return $broker;
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
            endpoint: $controller->endpoint,
            versions: $controller->versions,
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
     * @param non-empty-list<Config\Endpoint> $hosts
     * @throws KafkaException
     */
    private function connectToBroker(
        array $hosts,
        Cancellation $cancellation,
    ): Cluster\Broker {
        $context = new Socket\ConnectContext()
            ->withConnectTimeout($this->config->connectTimeout->toSeconds())
            ->withTcpNoDelay();

        if (($tls = $this->config->tls?->context()) !== null) {
            $context = $context->withTlsContext($tls);
        }

        return $this->bootstrapper->connect(
            $hosts,
            $context,
            $this->versions,
            $cancellation,
        );
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

use Amp\Cancellation;
use Amp\Socket;
use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Exception\ConnectionFailed;
use Thesis\Kafka\Runtime\Cluster\Broker;
use Thesis\Kafka\Runtime\Cluster\NegotiatedVersions;
use Thesis\Kafka\Runtime\Connection;

/**
 * @internal
 */
final readonly class Bootstrapper
{
    private Socket\SocketConnector $connector;

    public function __construct(
        ?Socket\SocketConnector $connector = null,
    ) {
        $this->connector = $connector ?? Socket\socketConnector();
    }

    /**
     * @param non-empty-list<Endpoint> $endpoints
     * @throws ConnectionFailed
     */
    public function connect(
        array $endpoints,
        Socket\ConnectContext $context,
        NegotiatedVersions $versions,
        Cancellation $cancellation,
    ): Broker {
        $reasons = [];

        foreach ($endpoints as $endpoint) {
            try {
                $socket = $this->connector->connect(
                    self::formatEndpoint($endpoint),
                    $context,
                    $cancellation,
                );

                // This is a temporary bootstrap broker entry. At this point we only have
                // a live connection to one seed endpoint, but we do not yet know which
                // broker ID it corresponds to. A subsequent MetadataRequest will either
                // match this connection to a real broker ID or cause us to discard it in
                // favor of a new connection to an advertised broker endpoint.
                return Broker::unknown(
                    new Connection($socket),
                    $endpoint,
                    $versions,
                );
            } catch (\Throwable $e) {
                $reasons[(string) $endpoint] = $e;
            }
        }

        throw new ConnectionFailed($reasons);
    }

    /**
     * @return non-empty-string
     */
    private static function formatEndpoint(Endpoint $endpoint): string
    {
        $url = (string) $endpoint;

        if (!str_starts_with($url, 'tcp://')) {
            $url = "tcp://{$url}";
        }

        return $url;
    }
}

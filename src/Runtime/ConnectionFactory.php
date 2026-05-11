<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Amp\Cancellation;
use Amp\NullCancellation;
use Amp\Socket;
use Thesis\Kafka\Config;
use Thesis\Kafka\Exception\ConnectionFailed;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;

/**
 * Opens new TCP/TLS connections to Kafka brokers and runs the per-
 * connection bootstrap protocol: {@see Generated\ApiVersionsRequest} → SASL handshake
 * (when configured). Returns a {@see Connection} that is ready for arbitrary requests.
 *
 * Stateless beyond the prebuilt {@see ConnectContext}: one factory is shared
 * by every Broker in the client.
 *
 * @internal
 */
final readonly class ConnectionFactory
{
    private Socket\ConnectContext $context;

    private Socket\SocketConnector $connector;

    public function __construct(
        private Config $config,
        ?Socket\SocketConnector $connector = null,
    ) {
        $this->context = self::configureConnectContext($config);
        $this->connector = $connector ?? Socket\socketConnector();
    }

    /**
     * @throws KafkaException
     */
    public function open(
        Config\Endpoint $endpoint,
        Cancellation $cancellation = new NullCancellation(),
    ): Connection {
        try {
            $socket = $this->connector->connect(
                self::formatEndpoint($endpoint),
                $this->context,
                $cancellation,
            );
        } catch (\Throwable $e) {
            throw new ConnectionFailed($endpoint, $e);
        }

        $connection = new Connection(
            $socket,
            $endpoint,
        );

        $this->doVersionsHandshake($connection, $cancellation);

        return $connection;
    }

    /**
     * @throws KafkaException
     */
    private function doVersionsHandshake(Connection $connection, Cancellation $cancellation): void
    {
        // ApiVersions is the only request we can send before the connection has
        // any negotiated protocol state. We start at v4 because modern brokers
        // support it and can return richer version metadata. If the broker
        // answers with UNSUPPORTED_VERSION, we fall back once to v0 — the
        // conservative baseline expected to work on older brokers.
        //
        // The Connection read path already knows how to reinterpret the same
        // ApiVersions response frame across legacy and modern body shapes, so
        // this method only decides whether a second wire request is necessary.
        //
        // We do not share negotiated versions across connections: each new
        // socket performs its own bootstrap and seeds its own version map
        // (KIP-511).
        $request = new Generated\ApiVersionsRequest(
            clientSoftwareName: $this->config->clientSoftwareName,
            clientSoftwareVersion: $this->config->clientSoftwareVersion,
        );

        $versions = [4, 0];
        $last = array_key_last($versions);

        foreach ($versions as $index => $version) {
            $response = $connection
                ->call(
                    new Connection\Call(
                        request: $request,
                        requestVersion: $version,
                        responseVersion: 0,
                    ),
                    $cancellation,
                );

            $error = ErrorCode::from($response->errorCode);
            if ($error === ErrorCode::None) {
                $connection->versions->seed(...$response->apiKeys);

                return;
            }

            if ($error !== ErrorCode::UnsupportedVersion || $index === $last) {
                throw new ProtocolError(Errors::for($response));
            }
        }
    }

    private static function configureConnectContext(Config $config): Socket\ConnectContext
    {
        // TCP_NODELAY (disable Nagle's algorithm): Kafka clients are
        // request/response over a single TCP and don't benefit from
        // buffering small writes. Nagle adds latency without throughput
        // gain in this access pattern.
        $context = new Socket\ConnectContext()
            ->withConnectTimeout($config->connectTimeout->toSeconds())
            ->withTcpNoDelay();

        if ($config->tls !== null) {
            $context = $context->withTlsContext(self::configureTLS($config->tls));
        }

        return $context;
    }

    private static function configureTLS(Config\TLSConfig $tls): Socket\ClientTlsContext
    {
        $context = new Socket\ClientTlsContext()
            ->withMinimumVersion($tls->minVersion);

        if (!$tls->verifyPeer) {
            $context = $context->withoutPeerVerification();
        } elseif (!$tls->verifyPeerName) {
            $context = $context->withoutPeerNameVerification();
        }

        if ($tls->caFile !== null) {
            $context = $context->withCaFile($tls->caFile);
        }

        if ($tls->caPath !== null) {
            $context = $context->withCaPath($tls->caPath);
        }

        if ($tls->certificateFile !== null && $tls->privateKeyFile !== null) {
            $context = $context->withCertificate(new Socket\Certificate(
                $tls->certificateFile,
                $tls->privateKeyFile,
                $tls->privateKeyPassword,
            ));
        }

        return $context;
    }

    /**
     * @return non-empty-string
     */
    private static function formatEndpoint(Config\Endpoint $endpoint): string
    {
        $url = (string) $endpoint;

        if (!str_starts_with($url, 'tcp://')) {
            $url = "tcp://{$url}";
        }

        return $url;
    }
}

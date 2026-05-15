<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Amp\Cancellation;
use Amp\NullCancellation;
use Amp\Socket;
use Thesis\Kafka\Config;
use Thesis\Kafka\Exception\ConnectionFailed;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\SaslAuthenticationFailed;
use Thesis\Kafka\KafkaException;
use Thesis\Kafka\Protocol\ErrorCode;
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
            $this->config,
            $endpoint,
        );

        $connection->versionsHandshake($cancellation);

        try {
            $connection->authenticate($cancellation);
        } catch (\Throwable $e) {
            $connection->close();

            if ($e instanceof ProtocolError) {
                $authenticationErrors = $e->errors->ofCode(ErrorCode::SaslAuthenticationFailed);
                if ($authenticationErrors !== []) {
                    $e = new SaslAuthenticationFailed($authenticationErrors[0]->message());
                }
            }

            throw $e;
        }

        return $connection;
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

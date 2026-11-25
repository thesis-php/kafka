<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Socket;
use Thesis\Kafka\Config\Endpoint;
use function Amp\async;

/**
 * A TCP proxy in the test process. It copies bytes between the client and a
 * real broker and knows nothing about the Kafka protocol. A test calls
 * {@see disconnect()} to make the broker unreachable for the client.
 * Create it with {@see Kafka::proxy()}.
 *
 * @internal
 */
final class Proxy
{
    public readonly Endpoint $endpoint;

    private readonly Socket\ServerSocket $server;

    /** @var list<Socket\Socket> */
    private array $sockets = [];

    private bool $disconnected = false;

    public function __construct(
        private readonly Endpoint $upstream,
    ) {
        $this->server = Socket\listen('127.0.0.1:0');

        $address = $this->server->getAddress();
        \assert($address instanceof Socket\InternetAddress);
        $port = $address->getPort();
        \assert($port > 0);
        $this->endpoint = new Endpoint('127.0.0.1', $port);

        async($this->accept(...))->ignore();
    }

    /**
     * Closes every open connection. After this call the proxy accepts a new
     * connection and closes it at once, so the client fails fast.
     */
    public function disconnect(): void
    {
        $this->disconnected = true;

        foreach ($this->sockets as $socket) {
            $socket->close();
        }

        $this->sockets = [];
    }

    /**
     * Closes the listener and every open connection.
     */
    public function close(): void
    {
        $this->server->close();
        $this->disconnect();
    }

    private function accept(): void
    {
        while (($client = $this->server->accept()) !== null) {
            if ($this->disconnected) {
                $client->close();

                continue;
            }

            $this->sockets[] = $client;

            try {
                $broker = Socket\connect((string) $this->upstream);
            } catch (\Throwable) {
                $client->close();

                continue;
            }

            $this->sockets[] = $broker;

            async(self::copy(...), $client, $broker)->ignore();
            async(self::copy(...), $broker, $client)->ignore();
        }
    }

    private static function copy(Socket\Socket $from, Socket\Socket $to): void
    {
        try {
            while (($chunk = $from->read()) !== null) {
                $to->write($chunk);
            }
        } catch (\Throwable) {
            // One side is gone. Closing both sides below is enough.
        } finally {
            $from->close();
            $to->close();
        }
    }
}

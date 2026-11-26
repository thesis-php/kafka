<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Socket;
use Thesis\Kafka\Config\Endpoint;
use function Amp\async;

/**
 * A TCP proxy in the test process. It copies bytes between the client and a
 * real broker and knows nothing about the Kafka protocol. A test calls
 * {@see disconnect()} to make the broker unreachable for the client, or
 * {@see stall()} to make the open connections silent.
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

    /** @var \SplObjectStorage<Socket\Socket, null> */
    private readonly \SplObjectStorage $stalled;

    private bool $disconnected = false;

    private int $accepted = 0;

    /**
     * @param int<0, 65535> $port 0 is a free port
     */
    public function __construct(
        private readonly Endpoint $upstream,
        int $port = 0,
    ) {
        $this->server = Socket\listen("127.0.0.1:{$port}");
        $this->stalled = new \SplObjectStorage();

        $address = $this->server->getAddress();
        \assert($address instanceof Socket\InternetAddress);
        $port = $address->getPort();
        \assert($port > 0);
        $this->endpoint = new Endpoint('127.0.0.1', $port);

        async($this->accept(...))->ignore();
    }

    /**
     * The number of client connections that the proxy has accepted.
     */
    public function accepted(): int
    {
        return $this->accepted;
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
     * Stops copying bytes on every open connection. The connections stay
     * open, so for the client the broker is alive but never answers.
     * New connections work as before.
     */
    public function stall(): void
    {
        foreach ($this->sockets as $socket) {
            $this->stalled->offsetSet($socket);
        }
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
            ++$this->accepted;

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

            async($this->copy(...), $client, $broker)->ignore();
            async($this->copy(...), $broker, $client)->ignore();
        }
    }

    private function copy(Socket\Socket $from, Socket\Socket $to): void
    {
        try {
            while (($chunk = $from->read()) !== null) {
                if (!$this->stalled->contains($from)) {
                    $to->write($chunk);
                }
            }
        } catch (\Throwable) {
            // One side is gone. Closing both sides below is enough.
        } finally {
            $from->close();
            $to->close();
        }
    }
}

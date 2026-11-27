<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Socket;
use Thesis\Kafka\Config\Endpoint;
use function Amp\async;
use function Amp\delay;

/**
 * A TCP proxy in the test process. It copies bytes between the client and a
 * real broker and knows nothing about the Kafka protocol. A test calls
 * {@see disconnect()} to make the broker unreachable for the client,
 * {@see reconnect()} to make it reachable again,
 * {@see drop()} to close the open connections, {@see stall()} to make the
 * open connections silent, {@see silence()} to make also the new connections
 * silent, or {@see delayAnswers()} to make the broker slow.
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

    private bool $silent = false;

    private float $answerDelay = 0.0;

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
        $this->drop();
    }

    /**
     * Undoes {@see disconnect()}: new connections reach the broker again.
     */
    public function reconnect(): void
    {
        $this->disconnected = false;
    }

    /**
     * Closes every open connection. New connections work as before.
     */
    public function drop(): void
    {
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
     * Stops copying bytes on every open connection and on every new one.
     * The proxy still accepts connections, but the broker never answers.
     */
    public function silence(): void
    {
        $this->silent = true;
        $this->stall();
    }

    /**
     * Each answer of the broker reaches the client this number of seconds later.
     */
    public function delayAnswers(float $seconds): void
    {
        $this->answerDelay = $seconds;
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

            if ($this->silent) {
                $this->stalled->offsetSet($client);
                $this->stalled->offsetSet($broker);
            }

            async($this->copy(...), $client, $broker, false)->ignore();
            async($this->copy(...), $broker, $client, true)->ignore();
        }
    }

    /**
     * @param bool $answers the bytes go from the broker to the client
     */
    private function copy(Socket\Socket $from, Socket\Socket $to, bool $answers): void
    {
        try {
            while (($chunk = $from->read()) !== null) {
                if ($answers && $this->answerDelay > 0) {
                    delay($this->answerDelay);
                }

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

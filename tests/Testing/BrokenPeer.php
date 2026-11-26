<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Socket;
use Thesis\Kafka\Config\Endpoint;
use function Amp\async;

/**
 * A TCP server in the test process that plays a broken broker. It accepts
 * connections and then closes them, stalls, sends garbage, or sends a bad
 * varint, as its {@see PeerFault} says. It does not speak the Kafka protocol,
 * except for one fixed ApiVersions answer: it checks only how the client
 * reacts to a broken peer. Create it with {@see Kafka::brokenPeer()}.
 *
 * @internal
 */
final class BrokenPeer
{
    /**
     * The garbage after the frame length. It is much larger than the memory
     * margin of a test, so a client that keeps it is easy to see.
     */
    public const int GARBAGE_BYTES = 32 << 20;
    private const int GARBAGE_CHUNK_BYTES = 1 << 20;

    /**
     * The frame length that {@see PeerFault::Garbage} announces: 2 GiB.
     */
    private const int GARBAGE_FRAME_LENGTH = 1 << 31;

    /**
     * The body of an ApiVersions v4 response: no error, one key
     * (FindCoordinator, versions 3 to 4), no throttle, no tagged fields.
     * FindCoordinator v3 and later has a flexible response header, so the
     * client reads a varint first.
     */
    private const string API_VERSIONS_BODY = "\x00\x00\x02\x00\x0A\x00\x03\x00\x04\x00\x00\x00\x00\x00\x00";

    public readonly Endpoint $endpoint;

    private readonly Socket\ServerSocket $server;

    /** @var \SplObjectStorage<Socket\Socket, null> */
    private \SplObjectStorage $open;

    private int $accepted = 0;

    public function __construct(
        private readonly PeerFault $fault,
    ) {
        $this->server = Socket\listen('127.0.0.1:0');
        $this->open = new \SplObjectStorage();

        $address = $this->server->getAddress();
        \assert($address instanceof Socket\InternetAddress);
        $port = $address->getPort();
        \assert($port > 0);
        $this->endpoint = new Endpoint('127.0.0.1', $port);

        async($this->accept(...))->ignore();
    }

    /**
     * The number of connections that the peer has accepted.
     */
    public function accepted(): int
    {
        return $this->accepted;
    }

    /**
     * The number of accepted connections that the client has not closed yet.
     */
    public function openConnections(): int
    {
        return \count($this->open);
    }

    /**
     * Closes the listener and every open connection.
     */
    public function close(): void
    {
        $this->server->close();

        foreach ($this->open as $socket) {
            $socket->close();
        }

        $this->open = new \SplObjectStorage();
    }

    private function accept(): void
    {
        while (($client = $this->server->accept()) !== null) {
            ++$this->accepted;

            if ($this->fault === PeerFault::Close) {
                $client->close();

                continue;
            }

            $this->open->offsetSet($client);

            async($this->serve(...), $client)->ignore();
        }
    }

    private function serve(Socket\Socket $client): void
    {
        try {
            if ($this->fault === PeerFault::Garbage && $client->read() !== null) {
                $client->write(pack('N', self::GARBAGE_FRAME_LENGTH));

                $chunk = str_repeat("\xFF", self::GARBAGE_CHUNK_BYTES);

                for ($sent = 0; $sent < self::GARBAGE_BYTES; $sent += self::GARBAGE_CHUNK_BYTES) {
                    $client->write($chunk);
                }
            }

            if ($this->fault === PeerFault::CutVarint || $this->fault === PeerFault::LongVarint) {
                $this->answer($client, match ($this->fault) {
                    PeerFault::CutVarint => "\x80",
                    PeerFault::LongVarint => str_repeat("\xFF", 10) . "\x01",
                });
            }

            // Read until the client closes the connection, so the peer knows when it is gone.
            while ($client->read() !== null);
        } catch (\Throwable) {
            // The client has closed the connection.
        } finally {
            $client->close();
            $this->open->offsetUnset($client);
        }
    }

    /**
     * Answers the first request with {@see self::API_VERSIONS_BODY} and
     * every next request with the bad body.
     */
    private function answer(Socket\Socket $client, string $badBody): void
    {
        $buffer = '';
        $answered = 0;

        while (($chunk = $client->read()) !== null) {
            $buffer .= $chunk;

            // A request frame: length, api key, api version, correlation id.
            while (\strlen($buffer) >= 12) {
                /** @var array{1: int} $length */
                $length = unpack('N', $buffer);
                if (\strlen($buffer) < 4 + $length[1]) {
                    break;
                }

                $body = $answered++ === 0 ? self::API_VERSIONS_BODY : $badBody;
                $client->write(pack('N', 4 + \strlen($body)) . substr($buffer, 8, 4) . $body);

                $buffer = substr($buffer, 4 + $length[1]);
            }
        }
    }
}

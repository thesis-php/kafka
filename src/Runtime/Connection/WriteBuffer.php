<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

use Amp\Socket\Socket;
use Thesis\Kafka\Protocol\Generated\RequestHeader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @internal
 */
final readonly class WriteBuffer
{
    public function __construct(
        private Socket $socket,
    ) {}

    /**
     * @param PendingRequest<*> $request
     */
    public function write(PendingRequest $request): void
    {
        $apiKey = $request->call->request->apiKey();
        $apiVersion = $request->version;
        $headerVersion = $apiKey->requestHeaderVersion($apiVersion);

        $payload = new Writer();

        $header = new RequestHeader(
            requestApiKey: $apiKey->value,
            requestApiVersion: $apiVersion,
            correlationId: $request->correlationId,
            clientId: null,
        );

        $header->encode($payload, $headerVersion);

        $request->call->request->encode($payload, $apiVersion);

        $frame = new Writer();
        $frame->writeInt32(\count($payload));

        $buffer = (string) $frame . (string) $payload;
        $this->socket->write($buffer);
    }
}

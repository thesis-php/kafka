<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

use Amp\DeferredFuture;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Protocol\Reader;

/**
 * @internal
 * @template R of Generated\Response
 */
final readonly class PendingRequest
{
    /**
     * @param Call<R> $call
     * @param DeferredFuture<R> $deferred
     * @param non-negative-int $correlationId
     * @param non-negative-int $version
     */
    public function __construct(
        public Call $call,
        public DeferredFuture $deferred,
        public int $correlationId,
        public int $version,
    ) {}

    public function complete(Reader $reader): void
    {
        /** @var class-string<R> $responseType */
        $responseType = $this->call->request->responseType();

        $response = $responseType::decode($reader, $this->version);

        $this->deferred->complete($response);
    }

    public function error(\Throwable $e): void
    {
        $this->deferred->error($e);
    }
}

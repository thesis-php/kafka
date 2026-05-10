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

    /**
     * @return R
     */
    public function decodeResponse(Reader $reader): Generated\Response
    {
        /** @var class-string<R> $responseType */
        $responseType = $this->call->request->responseType();

        return $responseType::decode($reader, $this->version);
    }

    /**
     * @param ?R $response
     */
    public function complete(?Generated\Response $response = null, ?\Throwable $exception = null): void
    {
        if ($exception !== null) {
            $this->deferred->error($exception);
        } else {
            \assert($response !== null);
            $this->deferred->complete($response);
        }
    }
}

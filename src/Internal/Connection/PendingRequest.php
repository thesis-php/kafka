<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

use Amp\DeferredFuture;
use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 * @template R of Generated\Response
 */
final readonly class PendingRequest
{
    /**
     * @param Generated\Request<R> $request
     * @param DeferredFuture<R> $deferred
     * @param non-negative-int $correlationId
     * @param non-negative-int $requestVersion
     * @param non-negative-int $responseVersion
     */
    public function __construct(
        public Generated\Request $request,
        public DeferredFuture $deferred,
        public int $correlationId,
        public int $requestVersion,
        public int $responseVersion,
    ) {}

    /**
     * @param R $response
     */
    public function complete(Generated\Response $response): void
    {
        $this->deferred->complete($response);
    }

    public function error(\Throwable $e): void
    {
        $this->deferred->error($e);
    }
}

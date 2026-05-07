<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Internal\Connection;

use Amp\DeferredFuture;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Runtime\Internal\Call;

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
     */
    public function __construct(
        public Call $call,
        public DeferredFuture $deferred,
        public int $correlationId,
    ) {}
}

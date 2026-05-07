<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Internal;

use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 * @template T of Generated\Response
 */
final readonly class Call
{
    /**
     * @param Generated\Request<T> $request
     * @param non-negative-int $version
     */
    public function __construct(
        public Generated\Request $request,
        public int $version,
    ) {}
}

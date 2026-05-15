<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 * @template T of Generated\Response
 */
final readonly class Call
{
    /**
     * Sentinel for "no explicit version, resolve via NegotiatedVersions".
     * We can't use null because the type is non-negative-int:
     * a real version is always >= 0 by the wire protocol.
     */
    private const int NOT_A_REAL_VERSION = -1;

    /**
     * @param Generated\Request<T> $request
     * @param -1|non-negative-int $requestVersion
     * @param -1|non-negative-int $responseVersion
     */
    public function __construct(
        public Generated\Request $request,
        public int $requestVersion = self::NOT_A_REAL_VERSION,
        public int $responseVersion = self::NOT_A_REAL_VERSION,
    ) {}

    /**
     * @phpstan-assert-if-false non-negative-int $this->requestVersion
     */
    public function requestVersionUnset(): bool
    {
        return $this->requestVersion === self::NOT_A_REAL_VERSION;
    }

    /**
     * @phpstan-assert-if-false non-negative-int $this->responseVersion
     */
    public function responseVersionUnset(): bool
    {
        return $this->responseVersion === self::NOT_A_REAL_VERSION;
    }
}

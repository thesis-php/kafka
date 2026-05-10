<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

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
     * @param -1|non-negative-int $version
     */
    public function __construct(
        public Generated\Request $request,
        public int $version = self::NOT_A_REAL_VERSION,
    ) {}

    public function versionUnset(): bool
    {
        return $this->version === self::NOT_A_REAL_VERSION;
    }
}

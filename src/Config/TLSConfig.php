<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Amp\Socket;

/**
 * @api
 */
final readonly class TLSConfig
{
    private const int TLS_MIN_DEFAULT_VERSION = Socket\ClientTlsContext::TLSv1_2;

    /**
     * @param ?non-empty-string $caFile
     * @param ?non-empty-string $caPath
     * @param ?non-empty-string $certificateFile
     * @param ?non-empty-string $privateKeyFile
     * @param ?non-empty-string $privateKeyPassword
     * @param Socket\ClientTlsContext::TLSv1_* $minVersion
     */
    public function __construct(
        public bool $verifyPeer = true,
        public bool $verifyPeerName = true,
        public ?string $caFile = null,
        public ?string $caPath = null,
        public ?string $certificateFile = null,
        public ?string $privateKeyFile = null,
        public ?string $privateKeyPassword = null,
        public int $minVersion = self::TLS_MIN_DEFAULT_VERSION,
    ) {}
}

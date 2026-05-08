<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;

/**
 * @api
 */
final readonly class TLSConfig
{
    /**
     * @param ?non-empty-string $caFile
     * @param ?non-empty-string $caPath
     * @param ?non-empty-string $certificateFile
     * @param ?non-empty-string $privateKeyFile
     * @param ?non-empty-string $privateKeyPassword
     */
    public function __construct(
        public bool $verifyPeer = true,
        public bool $verifyPeerName = true,
        public ?string $caFile = null,
        public ?string $caPath = null,
        public ?string $certificateFile = null,
        public ?string $privateKeyFile = null,
        public ?string $privateKeyPassword = null,
    ) {}

    public function context(): ClientTlsContext
    {
        $context = new ClientTlsContext();

        if (!$this->verifyPeer) {
            $context = $context->withoutPeerVerification();
        } elseif (!$this->verifyPeerName) {
            $context = $context->withoutPeerNameVerification();
        }

        if ($this->caFile !== null) {
            $context = $context->withCaFile($this->caFile);
        }

        if ($this->caPath !== null) {
            $context = $context->withCaPath($this->caPath);
        }

        if ($this->certificateFile !== null && $this->privateKeyFile !== null) {
            $context = $context->withCertificate(new Certificate(
                $this->certificateFile,
                $this->privateKeyFile,
                $this->privateKeyPassword,
            ));
        }

        return $context;
    }
}

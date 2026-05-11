<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Scram;

/**
 * @api
 */
final readonly class Sha256ScramAuthenticator extends ScramAuthenticator
{
    #[\Override]
    protected function algorithm(): HashAlgorithm
    {
        return HashAlgorithm::SHA256;
    }
}

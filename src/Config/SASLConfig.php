<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Thesis\Kafka\SASL;

/**
 * @api
 */
final readonly class SASLConfig
{
    /**
     * @param non-empty-list<SASL\Authenticator> $authenticators
     */
    public function __construct(
        public array $authenticators,
    ) {}
}

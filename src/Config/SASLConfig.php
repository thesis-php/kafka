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
     * @param non-empty-string $username
     * @param non-empty-string $password
     */
    public function __construct(
        public SASL\Mechanism $mechanism,
        public string $username,
        public string $password,
    ) {}
}

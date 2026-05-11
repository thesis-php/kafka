<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

/**
 * @api
 */
final readonly class Exchange
{
    /**
     * @param non-empty-string $message
     */
    public function __construct(
        public State $state,
        public string $message,
    ) {}
}

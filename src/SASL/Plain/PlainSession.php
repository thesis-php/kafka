<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Plain;

use Thesis\Kafka\SASL;

/**
 * @internal
 */
final readonly class PlainSession implements SASL\Session
{
    /**
     * @param non-empty-string $token
     */
    public function __construct(
        private string $token,
    ) {}

    #[\Override]
    public function start(): SASL\Exchange
    {
        return SASL\Exchange::done($this->token);
    }

    #[\Override]
    public function challenge(int $step, string $bytes): SASL\Exchange
    {
        throw new \LogicException('PLAIN does not produce challenges.');
    }
}

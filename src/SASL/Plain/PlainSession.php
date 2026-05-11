<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Plain;

use Thesis\Kafka\SASL\Exchange;
use Thesis\Kafka\SASL\Session;
use Thesis\Kafka\SASL\State;

/**
 * @internal
 */
final readonly class PlainSession implements Session
{
    /**
     * @param non-empty-string $token
     */
    public function __construct(
        private string $token,
    ) {}

    #[\Override]
    public function start(): Exchange
    {
        return new Exchange(State::Done, $this->token);
    }

    #[\Override]
    public function challenge(int $step, string $bytes): Exchange
    {
        throw new \LogicException('PLAIN does not produce challenges.');
    }
}

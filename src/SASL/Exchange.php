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
    public static function continue(string $message): self
    {
        return new self(State::Continue, $message);
    }

    /**
     * @param ?non-empty-string $message
     */
    public static function done(?string $message = null): self
    {
        return new self(State::Done, $message);
    }

    /**
     * @param ?non-empty-string $message
     */
    private function __construct(
        public State $state,
        public ?string $message = null,
    ) {}
}

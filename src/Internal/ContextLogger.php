<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Psr\Log\LoggerInterface;
use Psr\Log\LoggerTrait;
use Psr\Log\NullLogger;

/**
 * @internal
 */
final readonly class ContextLogger implements LoggerInterface
{
    use LoggerTrait;

    /**
     * @param array<string, mixed> $context
     */
    public function __construct(
        private LoggerInterface $logger,
        private array $context = [],
    ) {}

    public static function null(): self
    {
        return new self(new NullLogger());
    }

    #[\Override]
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        // @infection-ignore-all
        $this->logger->log($level, $message, [
            ...$this->context,
            ...$context,
        ]);
    }

    /**
     * Derive a logger carrying additional fixed context on top of this one.
     *
     * @param array<string, mixed> $context
     */
    public function with(array $context): self
    {
        // @infection-ignore-all
        return new self($this->logger, [
            ...$this->context,
            ...$context,
        ]);
    }
}

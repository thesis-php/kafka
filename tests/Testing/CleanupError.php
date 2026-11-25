<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

/**
 * @internal
 */
final readonly class CleanupError
{
    /**
     * @param non-empty-string $step
     */
    public function __construct(
        public string $step,
        public \Throwable $error,
    ) {}
}

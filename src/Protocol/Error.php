<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 */
final readonly class Error
{
    public function __construct(
        public ErrorCode $code,
        public ?string $message = null,
    ) {}
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
final readonly class RecordHeader
{
    /**
     * @param non-empty-string $key
     * @param ?string $value raw bytes, null is allowed and distinct from empty string
     */
    public function __construct(
        public string $key,
        public ?string $value = null,
    ) {}
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class PartitionError implements Error
{
    public function __construct(
        public string $topic,
        public int $partition,
        private ErrorCode $code,
        private ?string $message = null,
    ) {}

    #[\Override]
    public function code(): ErrorCode
    {
        return $this->code;
    }

    #[\Override]
    public function message(): string
    {
        return \sprintf(
            "topic '%s' partition %d: %s",
            $this->topic,
            $this->partition,
            $this->message ?? $this->code->description(),
        );
    }
}

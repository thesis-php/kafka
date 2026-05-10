<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class ConfigResourceError implements Error
{
    public function __construct(
        public int $resourceType,
        public string $resourceName,
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
            "config resource type=%d name='%s': %s",
            $this->resourceType,
            $this->resourceName,
            $this->message ?? $this->code->description(),
        );
    }
}

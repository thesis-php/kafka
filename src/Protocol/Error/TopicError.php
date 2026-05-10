<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class TopicError implements Error
{
    public function __construct(
        public string $topic,
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
            "topic '%s': %s",
            $this->topic,
            $this->message ?? $this->code->description(),
        );
    }
}

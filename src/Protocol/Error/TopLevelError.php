<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class TopLevelError implements Error
{
    public function __construct(
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
        $message = $this->message ?? $this->code->description();
        if ($message === '') {
            $message = 'Unknown error.';
        }

        return $message;
    }
}

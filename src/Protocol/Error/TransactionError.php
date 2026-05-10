<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class TransactionError implements Error
{
    public function __construct(
        public string $transactionalId,
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
            "transaction '%s': %s",
            $this->transactionalId,
            $this->message ?? $this->code->description(),
        );
    }
}

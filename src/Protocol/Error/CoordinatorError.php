<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class CoordinatorError implements Error
{
    public function __construct(
        public int $nodeId,
        public string $host,
        public int $port,
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
            'coordinator %s:%d: %s',
            $this->host,
            $this->port,
            $this->message ?? $this->code->description(),
        );
    }
}

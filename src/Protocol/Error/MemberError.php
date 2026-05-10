<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Error;

use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;

/**
 * @api
 */
final readonly class MemberError implements Error
{
    public function __construct(
        public string $groupId,
        public string $memberId,
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
            "group '%s' member '%s': %s",
            $this->groupId,
            $this->memberId,
            $this->message ?? $this->code->description(),
        );
    }
}

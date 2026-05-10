<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 */
interface Error
{
    public function code(): ErrorCode;

    /**
     * @return non-empty-string
     */
    public function message(): string;
}

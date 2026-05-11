<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

/**
 * @api
 */
interface Session
{
    public function start(): Exchange;

    /**
     * @param non-negative-int $step
     */
    public function challenge(int $step, string $bytes): Exchange;
}

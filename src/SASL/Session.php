<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

use Thesis\Kafka\Exception\SaslAuthenticationFailed;

/**
 * @api
 */
interface Session
{
    public function start(): Exchange;

    /**
     * @param non-negative-int $step
     * @throws SaslAuthenticationFailed
     */
    public function challenge(int $step, string $bytes): Exchange;
}

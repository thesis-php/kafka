<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

/**
 * @api
 */
interface Authenticator
{
    /**
     * @return non-empty-string
     */
    public function mechanism(): string;

    public function authenticate(): Session;
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Plain;

use Thesis\Kafka\SASL\Authenticator;
use Thesis\Kafka\SASL\Session;

/**
 * @api
 */
final readonly class PlainAuthenticator implements Authenticator
{
    /**
     * @param non-empty-string $username
     * @param non-empty-string $password
     */
    public function __construct(
        private string $username,
        private string $password,
        private string $zid = '',
    ) {}

    #[\Override]
    public function mechanism(): string
    {
        return 'PLAIN';
    }

    #[\Override]
    public function authenticate(): Session
    {
        return new PlainSession("{$this->zid}\0{$this->username}\0{$this->password}");
    }
}

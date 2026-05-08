<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

/**
 * @api
 */
enum Mechanism: string
{
    case Plain = 'PLAIN';
    case ScramSha256 = 'SCRAM-SHA-256';
    case ScramSha512 = 'SCRAM-SHA-512';
}

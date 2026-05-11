<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL;

/**
 * @api
 */
enum State
{
    case Continue;
    case Done;
}

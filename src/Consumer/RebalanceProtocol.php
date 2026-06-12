<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

/**
 * @api
 */
enum RebalanceProtocol
{
    case Eager;
    case Cooperative;
}

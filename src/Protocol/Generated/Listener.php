<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

/**
 * @api
 * @generated
 */
enum Listener: string
{
    case Broker = 'broker';
    case Controller = 'controller';
}

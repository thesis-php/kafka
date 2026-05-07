<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
enum Type: string
{
    case Request = 'request';
    case Response = 'response';
    case Header = 'header';
    case Data = 'data';
    case Metadata = 'metadata';
}

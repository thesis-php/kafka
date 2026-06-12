<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\ContextLogger;

/**
 * @internal
 */
enum ReleaseReasonContextKey
{
    case Manual;
    case Size;
    case Linger;
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

/**
 * @internal
 */
enum FlexibleKind
{
    case Always;
    case Never;
    case Gated;
}

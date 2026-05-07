<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

/**
 * @internal
 */
enum StaticNullableMode implements NullableMode
{
    case Never;
    case Always;

    #[\Override]
    public function apply(
        callable $nonNullable,
        callable $nullable,
        mixed $accessor,
    ): array {
        return match ($this) {
            self::Never => $nonNullable($accessor),
            self::Always => $nullable($accessor),
        };
    }
}

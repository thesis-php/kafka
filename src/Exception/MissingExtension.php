<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class MissingExtension extends Exception
{
    public function __construct(
        public readonly string $extension,
    ) {
        parent::__construct(
            "Required PHP extension is not loaded: {$extension}",
        );
    }
}

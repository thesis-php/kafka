<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class BatchFull extends Exception
{
    public function __construct()
    {
        parent::__construct(
            'Batch is full in manual-flush mode; call flush() and retry the produce().',
        );
    }
}

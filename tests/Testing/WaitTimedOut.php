<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Thesis\Kafka\Exception;
use Thesis\Time\TimeSpan;

/**
 * @internal
 */
final class WaitTimedOut extends Exception
{
    public function __construct(
        public readonly string $expected,
        public readonly string $seen,
        public readonly TimeSpan $timeout,
    ) {
        parent::__construct(\sprintf(
            'Waited %s s for %s. Last seen: %s.',
            $timeout->toSeconds(3),
            $expected,
            $seen,
        ));
    }
}

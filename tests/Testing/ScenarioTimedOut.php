<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Thesis\Kafka\Exception;
use Thesis\Time\TimeSpan;

/**
 * @internal
 */
final class ScenarioTimedOut extends Exception
{
    public function __construct(
        public readonly TimeSpan $timeout,
    ) {
        parent::__construct(\sprintf(
            'The scenario did not finish within its time limit of %s s.',
            $timeout->toSeconds(3),
        ));
    }
}

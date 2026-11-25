<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Thesis\Kafka\Exception;

/**
 * @internal
 */
final class CleanupFailed extends Exception
{
    /**
     * @param non-empty-list<CleanupError> $errors
     */
    public function __construct(
        public readonly array $errors,
    ) {
        parent::__construct(
            \sprintf(
                'The cleanup after the scenario failed: %s',
                implode('; ', array_map(
                    static fn(CleanupError $error) => \sprintf('%s: %s', $error->step, $error->error->getMessage()),
                    $errors,
                )),
            ),
            previous: $errors[0]->error,
        );
    }
}

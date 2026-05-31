<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Data\DataSet;
use Testo\Test;

#[Covers('Thesis\Kafka\Internal\backoff')]
final readonly class RetryTest
{
    /**
     * @param positive-int $attempt
     * @param array{positive-int, positive-int} $range
     */
    #[Test]
    #[DataSet([1, [0.05, 0.10]])]
    #[DataSet([2, [0.10, 0.20]])]
    #[DataSet([3, [0.20, 0.40]])]
    #[DataSet([4, [0.40, 0.80]])]
    #[DataSet([5, [0.50, 1.00]])]
    #[DataSet([6, [0.50, 1.00]])]
    #[DataSet([7, [0.50, 1.00]])]
    public function backoff(int $attempt, array $range): void
    {
        Assert::float(backoff($attempt))->between($range[0], $range[1]);
    }
}

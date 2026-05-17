<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;

#[Covers(CorrelationIds::class)]
final class CorrelationIdsTest
{
    #[Test]
    public function generateIds(): void
    {
        $ids = new CorrelationIds();

        for ($i = 1; $i < 100; ++$i) {
            Assert::same($ids->id, $i);
        }
    }
}

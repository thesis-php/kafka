<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Internal\Connection;

use Testo\Assert;
use Testo\Test;

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

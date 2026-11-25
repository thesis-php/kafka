<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Testo\Assert;
use Testo\Test;

#[Test]
final readonly class NoneTest
{
    public function roundTrip(): void
    {
        $in = 'xyz';

        Assert::same(None::Compressor->compress($in), $in);
        Assert::same(None::Compressor->decompress($in), $in);
    }
}

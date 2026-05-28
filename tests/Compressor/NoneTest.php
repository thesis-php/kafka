<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;

#[Test]
#[Covers(None::class)]
final readonly class NoneTest
{
    public function roundTrip(): void
    {
        $in = 'xyz';

        Assert::same(None::Compressor->compress($in), $in);
        Assert::same(None::Compressor->decompress($in), $in);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Time;

use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Data\DataSet;
use Testo\Test;

#[Covers(Duration::class)]
final class DurationTest
{
    /**
     * @param non-negative-int $reqMs
     * @param non-negative-int $reqDurationMs
     */
    #[Test]
    #[DataSet([10_000, 100, 5_000, 14_000, 14_000])]
    #[DataSet([10_000, 2_000, 5_000, 12_800, 12_800])]
    #[DataSet([10_000, 100, 60_000, 67_000, 68_800])]
    #[DataSet([10_000, 3_000, 1_000, 10_000, 10_000])]
    public function calculatesReauthenticateDeadline(
        int $reqMs,
        int $reqDurationMs,
        int $sessionLifetimeMs,
        int $minExpected,
        int $maxExpected,
    ): void {
        $expiration = calculateSessionExpiration(
            requestStartMs: $reqMs,
            requestDurationMs: $reqDurationMs,
            sessionLifetimeMs: $sessionLifetimeMs,
        );

        Assert::int($expiration->milliseconds)->between($minExpected, $maxExpected);
    }
}

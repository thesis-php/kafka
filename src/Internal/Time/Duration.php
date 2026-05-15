<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Time;

use Random\Randomizer;

/**
 * @internal
 */
final readonly class Duration
{
    public static function now(): self
    {
        /** @var positive-int $ms */
        $ms = intdiv((int) hrtime(true), 1_000_000);

        return new self($ms);
    }

    /**
     * @param non-negative-int $milliseconds
     */
    public function __construct(
        public int $milliseconds,
    ) {}

    /**
     * @return non-negative-int
     */
    public function elapsed(?self $since = null): int
    {
        $since ??= self::now();
        \assert($since->milliseconds >= $this->milliseconds);

        /** @var non-negative-int */
        return $since->milliseconds - $this->milliseconds;
    }

    public function inFuture(?self $comparator = null): bool
    {
        $comparator ??= self::now();

        return $this->milliseconds > $comparator->milliseconds;
    }
}

/**
 * @internal
 */
function now(): Duration
{
    return Duration::now();
}

/**
 * @see https://github.com/twmb/franz-go/blob/master/pkg/kgo/broker.go `broker.doSasl` for reference
 *
 * Kafka may return a finite SASL session lifetime after successful
 * authentication. We intentionally schedule re-authentication *before*
 * that lifetime expires rather than right at the limit:
 *   - network latency and broker processing already consumed part of the
 *     advertised lifetime before we even received it;
 *   - a little random pessimism spreads re-authentication across clients
 *     instead of creating synchronized spikes;
 *   - initiating re-authentication slightly early is safer than risking
 *     the broker closing the connection on the next application request.
 *
 * This logic lives in a small pure function on purpose. The calculation is
 * non-trivial, but it has no transport side effects: it only depends on the
 * request start time, the observed authenticate round-trip duration, and the
 * broker-provided session lifetime. Keeping it separate makes it easy to unit
 * test the deadline math without opening sockets or driving a full SASL flow.
 *
 * @internal
 * @param non-negative-int $requestStartMs
 * @param non-negative-int $requestDurationMs
 */
function calculateSessionExpiration(
    int $requestStartMs,
    int $requestDurationMs,
    int $sessionLifetimeMs,
): Duration {
    /** @var ?Randomizer $randomizer */
    static $randomizer;
    $randomizer ??= new Randomizer();

    $minPessimismMs = max(
        1000,
        $requestDurationMs * 1.1,
    );

    $random = $randomizer->getFloat(0.0, 1.0);
    $maxPessimismMs = $sessionLifetimeMs * (0.05 - 0.03 * $random);

    $pessimismMs = max($minPessimismMs, $maxPessimismMs);
    $reauthenticateInMs = $sessionLifetimeMs - (int) ceil($pessimismMs);
    $reauthenticateInMs = max($reauthenticateInMs, 0);

    return new Duration($requestStartMs + $reauthenticateInMs);
}

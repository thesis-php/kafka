<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use Thesis\Time\TimeSpan;
use function Amp\delay;

/**
 * A wait with a time limit. Create it with {@see Kafka::poll()}.
 *
 * @internal
 * @template T
 */
final readonly class Poll
{
    private const int DEFAULT_TIMEOUT_SECONDS = 10;
    private const float DEFAULT_INTERVAL_SECONDS = 0.2;
    private const string DEFAULT_EXPECTED = 'a value that the condition accepts';

    private TimeSpan $timeout;

    /**
     * @param \Closure(Cancellation): T $probe
     * @param Cancellation $stop ends the wait when the scenario is stopped
     * @param non-empty-string $expected
     * @param ?\Closure(T): string $describe
     * @param int|float $interval seconds between two probes
     */
    public function __construct(
        private \Closure $probe,
        private Cancellation $stop,
        private string $expected = self::DEFAULT_EXPECTED,
        ?TimeSpan $timeout = null,
        private ?\Closure $describe = null,
        private int|float $interval = self::DEFAULT_INTERVAL_SECONDS,
    ) {
        $this->timeout = $timeout ?? TimeSpan::fromSeconds(self::DEFAULT_TIMEOUT_SECONDS);
    }

    /**
     * Says what the wait waits for. The failure message shows it.
     *
     * @param non-empty-string $expected
     * @return self<T>
     */
    public function expecting(string $expected): self
    {
        return new self($this->probe, $this->stop, $expected, $this->timeout, $this->describe, $this->interval);
    }

    /**
     * @return self<T>
     */
    public function within(TimeSpan $timeout): self
    {
        return new self($this->probe, $this->stop, $this->expected, $timeout, $this->describe, $this->interval);
    }

    /**
     * Describes the last value of the probe for the failure message.
     *
     * @param \Closure(T): string $describe
     * @return self<T>
     */
    public function describedBy(\Closure $describe): self
    {
        return new self($this->probe, $this->stop, $this->expected, $this->timeout, $describe, $this->interval);
    }

    /**
     * Calls the probe until $condition accepts its result, and returns this result.
     * The probe gets a cancellation that ends with the wait.
     *
     * @param \Closure(T): bool $condition
     * @return T
     * @throws WaitTimedOut
     */
    public function until(\Closure $condition): mixed
    {
        $deadline = new TimeoutCancellation($this->timeout->toSeconds(3));
        $cancellation = new CompositeCancellation($this->stop, $deadline);
        $seen = 'nothing';

        while (true) {
            try {
                $value = ($this->probe)($cancellation);

                if ($condition($value)) {
                    return $value;
                }

                $seen = $this->describe === null ? get_debug_type($value) : ($this->describe)($value);

                $cancellation->throwIfRequested();
                delay($this->interval, cancellation: $cancellation);
            } catch (CancelledException $e) {
                if (!$deadline->isRequested()) {
                    throw $e;
                }

                throw new WaitTimedOut($this->expected, $seen, $this->timeout);
            }
        }
    }
}

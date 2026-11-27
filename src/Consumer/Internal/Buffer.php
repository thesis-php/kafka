<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\TimeoutCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Time\TimeSpan;

/**
 * The buffer between the fetch and the reader of a consumer. The fetch
 * pushes values, and {@see read()} gives them to one reader at a time.
 *
 * After {@see close()} or {@see fail()} the buffer gives no other value.
 * The error of {@see fail()} reaches one reader only. Then every
 * {@see read()} throws {@see Exception\ConsumerClosed}.
 *
 * @internal
 * @template T
 */
final class Buffer
{
    /** @var Pipeline\Queue<T> */
    private readonly Pipeline\Queue $queue;

    /** @var Pipeline\ConcurrentIterator<T> */
    private readonly Pipeline\ConcurrentIterator $iterator;

    /**
     * The number of values in the queue. The queue itself does not tell it.
     */
    private int $size = 0;

    /**
     * A value that a reader took from the queue after its cancellation had
     * fired. It goes to the next reader, so no value is lost.
     *
     * @var list<T>
     */
    private array $kept = [];

    /** @var ?\WeakReference<\Generator<int, non-empty-list<Consumer\Record>, mixed, void>> */
    private ?\WeakReference $reader = null;

    private bool $closed = false;

    private ?\Throwable $failure = null;

    /**
     * @param int $size how many values wait in the buffer before a push waits
     * @param TimeSpan $pollTimeout the wait of {@see Consumer\ConsumeMode::Poll} without a cancellation
     */
    public function __construct(
        int $size,
        private readonly TimeSpan $pollTimeout,
    ) {
        $this->queue = new Pipeline\Queue($size);
        $this->iterator = $this->queue->iterate();
    }

    /**
     * Waits while the buffer is full. A closed buffer drops the value, because
     * nobody reads it.
     *
     * @param T $value
     * @throws CancelledException
     */
    public function push(mixed $value, Cancellation $cancellation): void
    {
        if ($this->closed) {
            return;
        }

        ++$this->size;
        $this->queue->pushAsync($value)->ignore()->await($cancellation);
    }

    /**
     * @param \Closure(T): ?non-empty-list<Consumer\Record> $accept gives the records of a value, null drops the value
     * @return iterable<non-empty-list<Consumer\Record>>
     * @throws Exception\ConsumerClosed
     * @throws Exception\ConsumerBusy
     * @throws \Throwable the error that closed the buffer, once
     */
    public function read(
        Consumer\ConsumeMode $mode,
        ?Cancellation $cancellation,
        \Closure $accept,
    ): iterable {
        if ($this->closed) {
            $this->throwFailure();

            throw new Exception\ConsumerClosed();
        }

        // A reader that is never iterated runs no code. So the buffer is free
        // again when such a reader is destroyed, not only when its iteration ends.
        if ($this->reader?->get() !== null) {
            throw new Exception\ConsumerBusy();
        }

        $cancellation ??= match ($mode) {
            Consumer\ConsumeMode::Poll => new TimeoutCancellation($this->pollTimeout->toSeconds(3)),
            Consumer\ConsumeMode::Stream => new NullCancellation(),
        };

        $reader = $this->iterate($mode, $cancellation, $accept);
        $this->reader = \WeakReference::create($reader);

        return $reader;
    }

    /**
     * The reader that waits ends. The values in the buffer are dropped.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->kept = [];
        $this->queue->complete();
    }

    /**
     * Closes the buffer. The reader that waits, or the next reader, gets the error.
     */
    public function fail(\Throwable $error): void
    {
        if ($this->closed) {
            return;
        }

        $this->failure = $error;
        $this->close();
    }

    /**
     * @param \Closure(T): ?non-empty-list<Consumer\Record> $accept
     * @return \Generator<int, non-empty-list<Consumer\Record>, mixed, void>
     */
    private function iterate(
        Consumer\ConsumeMode $mode,
        Cancellation $cancellation,
        \Closure $accept,
    ): \Generator {
        try {
            $yielded = false;

            while (true) {
                $this->throwFailure();

                if ($this->closed || $cancellation->isRequested()) {
                    return;
                }

                // After the first batch Poll gives only what is in the buffer now.
                if ($yielded && $mode === Consumer\ConsumeMode::Poll && $this->kept === [] && $this->size === 0) {
                    return;
                }

                $value = $this->take($cancellation);

                // close() or the cancellation has ended the wait. The checks above end the loop.
                if ($value === []) {
                    continue;
                }

                if ($cancellation->isRequested()) {
                    $this->kept = $value;

                    continue;
                }

                $records = $accept($value[0]);

                if ($records !== null) {
                    $yielded = true;

                    yield $records;
                }
            }
        } finally {
            $this->reader = null;
        }
    }

    /**
     * @return list<T> one value, or no value when the wait has ended or the buffer is closed
     */
    private function take(Cancellation $cancellation): array
    {
        if ($this->kept !== []) {
            $kept = $this->kept;
            $this->kept = [];

            return $kept;
        }

        try {
            if (!$this->iterator->continue($cancellation)) {
                return [];
            }
        } catch (CancelledException) {
            return [];
        }

        --$this->size;
        $value = $this->iterator->getValue();

        // close() came after the value. A closed buffer gives no value.
        if ($this->closed) {
            return [];
        }

        return [$value];
    }

    private function throwFailure(): void
    {
        $failure = $this->failure;
        $this->failure = null;

        if ($failure !== null) {
            throw $failure;
        }
    }
}

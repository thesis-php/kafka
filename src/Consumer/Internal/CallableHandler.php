<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Future;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\ConsumerSession;
use Thesis\Kafka\ConsumerSubscription;
use Thesis\Kafka\Exception;
use function Amp\async;

/**
 * The loop runs until {@see close()} or an error, also when nobody keeps
 * this handle.
 *
 * @internal
 */
final class CallableHandler implements ConsumerSubscription
{
    /** @var ?Future<null> */
    private ?Future $handle = null;

    /**
     * The fiber of the loop while the loop runs.
     *
     * @var ?\Fiber<mixed, mixed, mixed, mixed>
     */
    private ?\Fiber $fiber = null;

    private bool $closed = false;

    public function __construct(
        private readonly Consumer $consumer,
    ) {}

    /**
     * @param callable(non-empty-list<Consumer\Record>, ConsumerSession): void $handler
     * @throws Exception\ConsumerBusy
     */
    public function handle(callable $handler): void
    {
        if ($this->handle !== null) {
            throw new Exception\ConsumerBusy();
        }

        $this->handle = async(function () use ($handler): void {
            $this->fiber = \Fiber::getCurrent();

            try {
                // A consumer that is closed before the loop starts would throw ConsumerClosed.
                if ($this->closed) {
                    return;
                }

                foreach ($this->consumer->consume() as $records) {
                    $handler($records, $this->consumer);
                }
            } finally {
                $this->fiber = null;
                $this->consumer->close();
            }
        });
    }

    #[\Override]
    public function close(): void
    {
        $this->closed = true;

        // A closed consumer gives no other batch, so the loop ends after the current batch.
        $this->consumer->close();

        // The handler waits for this call, so the loop cannot end before it returns.
        if ($this->fiber === null || \Fiber::getCurrent() !== $this->fiber) {
            $this->join();
        }
    }

    #[\Override]
    public function join(): void
    {
        $this->handle?->await();
    }
}

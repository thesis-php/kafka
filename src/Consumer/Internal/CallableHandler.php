<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Revolt\EventLoop;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\ConsumerSession;
use Thesis\Kafka\ConsumerSubscription;

/**
 * @internal
 */
final readonly class CallableHandler implements ConsumerSubscription
{
    private DeferredCancellation $cancel;

    /** @var DeferredFuture<null> */
    private DeferredFuture $completion;

    public function __construct(
        private Consumer $consumer,
    ) {
        $this->cancel = new DeferredCancellation();
        $this->completion = new DeferredFuture();
    }

    /**
     * @param callable(non-empty-list<Consumer\Record>, ConsumerSession): void $handler
     */
    public function handle(callable $handler): void
    {
        $consumer = $this->consumer;
        $completion = $this->completion;
        $cancellation = $this->cancel->getCancellation();

        EventLoop::queue(static function () use (
            $handler,
            $consumer,
            $completion,
            $cancellation
        ): void {
            try {
                foreach ($consumer->consume(cancellation: $cancellation) as $records) {
                    $handler($records, $consumer);
                }
            } catch (CancelledException) {
                // expected on close()
            } catch (\Throwable $e) {
                $completion->error($e);
            } finally {
                if (!$completion->isComplete()) {
                    $completion->complete();
                }

                $consumer->close();
            }
        });
    }

    #[\Override]
    public function close(): void
    {
        $this->cancel->cancel();
        $this->join();
    }

    #[\Override]
    public function join(): void
    {
        $this->completion->getFuture()->await();
    }

    public function __destruct()
    {
        $this->cancel->cancel();
    }
}

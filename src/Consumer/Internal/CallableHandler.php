<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\ConsumerSession;
use Thesis\Kafka\ConsumerSubscription;
use Thesis\Kafka\Exception;
use function Amp\async;

/**
 * @internal
 */
final class CallableHandler implements ConsumerSubscription
{
    private readonly DeferredCancellation $cancel;

    /** @var ?Future<mixed> */
    private ?Future $handle = null;

    public function __construct(
        private readonly Consumer $consumer,
    ) {
        $this->cancel = new DeferredCancellation();
    }

    /**
     * @param callable(non-empty-list<Consumer\Record>, ConsumerSession): void $handler
     * @throws Exception\ConsumerBusy
     */
    public function handle(callable $handler): void
    {
        if ($this->handle !== null) {
            throw new Exception\ConsumerBusy();
        }

        $consumer = $this->consumer;
        $cancellation = $this->cancel->getCancellation();

        $this->handle = async(static function () use (
            $handler,
            $consumer,
            $cancellation,
        ): void {
            try {
                foreach ($consumer->consume(cancellation: $cancellation) as $records) {
                    $handler($records, $consumer);
                }
            } catch (CancelledException) {
                // expected on close()
            } finally {
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
        $this->handle?->await();
    }

    public function __destruct()
    {
        $this->cancel->cancel();
    }
}

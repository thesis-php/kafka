<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\RequestTimeout;
use Thesis\Kafka\Internal\Retry;
use Thesis\Kafka\NetworkException;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

/**
 * The fetch of one assignment: a {@see Fetcher} per leader and one recovery loop.
 *
 * A fetcher gives a partition that it cannot read to the recovery. The
 * recovery waits, refreshes the metadata, resets the offset when it is out
 * of range, and gives the partition to the fetcher of its current leader.
 * The position stays the same, so no record is lost or read twice.
 *
 * An error that cannot be fixed stops every loop and goes to the owner of
 * the cycle.
 *
 * @internal
 */
final class FetchCycle
{
    /** @var array<non-negative-int, Fetcher> */
    private array $fetchers = [];

    /** @var array<int, Future<void>> */
    private array $loops = [];

    private int $nextLoop = 0;

    private readonly DeferredCancellation $stopped;

    /**
     * Wakes the recovery when a partition starts to wait.
     *
     * @var ?DeferredFuture<null>
     */
    private ?DeferredFuture $wake = null;

    /**
     * @param \Closure(non-empty-list<Consumer\Record>, Cancellation): void $deliver gets each batch, it waits while the buffer is full
     * @param \Closure(\Throwable): void $failed gets the error that stopped the cycle
     * @param TimeSpan $requestTimeout the request timeout of the client
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly SubscriptionState $subscription,
        private readonly OffsetLister $offsets,
        private readonly Protocol\RecordBatch $batch,
        private readonly Consumer\Config $config,
        private readonly TimeSpan $requestTimeout,
        private readonly \Closure $deliver,
        private readonly \Closure $failed,
        private readonly ContextLogger $logger,
    ) {
        $this->stopped = new DeferredCancellation();
    }

    /**
     * Tells whether a new attempt can fix the error. The caller first checks
     * its own cancellation, so a cancellation here is the time limit of a request.
     */
    public static function retriable(\Throwable $e): bool
    {
        return $e instanceof CancelledException
            || $e instanceof NetworkException
            || $e instanceof Exception\UnknownBroker
            || ($e instanceof Exception\ProtocolError && array_all(
                $e->errors->errors,
                static fn(Error $error) => $error->code()->retriable(),
            ));
    }

    public function start(): void
    {
        foreach ($this->subscription->leaders() as $leaderId) {
            $this->startFetcher($leaderId);
        }

        $this->startLoop($this->recover(...));
    }

    /**
     * The partition waits for a new attempt.
     */
    public function retry(TopicPartition $tp): void
    {
        $this->subscription->release($tp);
        $this->wakeRecovery();
    }

    /**
     * The position of the partition is out of range. It waits for a reset.
     */
    public function reset(TopicPartition $tp): void
    {
        $this->subscription->release($tp, reset: true);
        $this->wakeRecovery();
    }

    /**
     * Stops every loop. The owner of the cycle gets the error.
     */
    public function fail(\Throwable $error): void
    {
        if ($this->stopped->isCancelled()) {
            return;
        }

        // @infection-ignore-all
        $this->logger->error('fetch stopped by an error', [
            'exception' => $error,
        ]);

        $this->stopped->cancel();
        ($this->failed)($error);
    }

    /**
     * Stops every loop and waits until they end.
     */
    public function stop(): void
    {
        $this->stopped->cancel();

        // No loop starts after the stop, so this list is complete.
        Future\awaitAll($this->loops);
    }

    private function wakeRecovery(): void
    {
        $wake = $this->wake;
        $this->wake = null;
        $wake?->complete();
    }

    /**
     * @param non-negative-int $leaderId
     */
    private function startFetcher(int $leaderId): void
    {
        // A running fetcher reads the partitions of its leader again before each request.
        if (isset($this->fetchers[$leaderId])) {
            return;
        }

        $fetcher = new Fetcher(
            leaderId: $leaderId,
            cluster: $this->cluster,
            subscription: $this->subscription,
            cycle: $this,
            batch: $this->batch,
            config: $this->config,
            requestTimeout: $this->requestTimeout,
            deliver: $this->deliver,
            // @infection-ignore-all
            logger: $this->logger->with([
                'component' => 'consumer.fetcher',
                'nodeId' => $leaderId,
            ]),
        );

        $this->fetchers[$leaderId] = $fetcher;

        $this->startLoop(function (Cancellation $cancellation) use ($fetcher, $leaderId): void {
            try {
                $fetcher->run($cancellation);
            } finally {
                // The fetcher has seen that its leader has no partitions, in the
                // same tick. So the recovery starts a new one for a new partition.
                unset($this->fetchers[$leaderId]);
            }
        });
    }

    /**
     * @param \Closure(Cancellation): void $loop
     */
    private function startLoop(\Closure $loop): void
    {
        if ($this->stopped->isCancelled()) {
            return;
        }

        $id = $this->nextLoop++;
        $cancellation = $this->stopped->getCancellation();

        $this->loops[$id] = async(function () use ($loop, $id, $cancellation): void {
            try {
                $loop($cancellation);
            } finally {
                unset($this->loops[$id]);
            }
        });
    }

    /**
     * Never throws: an error that cannot be fixed stops the cycle.
     */
    private function recover(Cancellation $cancellation): void
    {
        $attempt = 0;

        try {
            while (!$cancellation->isRequested()) {
                if ($this->subscription->waiting() === []) {
                    $attempt = 0;
                    $this->wake = new DeferredFuture();
                    $this->wake->getFuture()->await($cancellation);

                    continue;
                }

                ++$attempt;

                // A pause before each attempt, so no error makes a loop of requests.
                delay(Retry\backoff($attempt), cancellation: $cancellation);

                try {
                    $this->resolve($cancellation);
                } catch (\Throwable $e) {
                    if ($cancellation->isRequested() || !self::retriable($e)) {
                        throw $e;
                    }

                    // @infection-ignore-all
                    $this->logger->warning('fetch recovery failed, retrying', [
                        'attempt' => $attempt,
                        'exception' => $e,
                    ]);
                }
            }
        } catch (\Throwable $e) {
            if (!$cancellation->isRequested()) {
                $this->fail($e);
            }
        }
    }

    /**
     * Gives each waiting partition to the fetcher of its current leader.
     * A partition without a leader waits for the next attempt.
     *
     * @throws Exception
     * @throws CancelledException
     */
    private function resolve(Cancellation $cancellation): void
    {
        $metadata = $this->cluster->refreshMetadata(
            $this->subscription->topics,
            RequestTimeout::start($this->requestTimeout->toSeconds(3), $cancellation),
        );

        /** @var list<TopicPartition> $lost */
        $lost = [];

        /** @var list<TopicPartition> $resets */
        $resets = [];

        foreach ($this->subscription->waiting() as $tp) {
            $leader = $metadata->topics[$tp->topic]->partitions[$tp->partition]->leader ?? -1;

            if ($leader < 0) {
                $lost[] = $tp;
            } elseif ($this->subscription->resetting($tp)) {
                $resets[] = $tp;
            } else {
                $this->subscription->follow($tp, $leader);
                $this->startFetcher($leader);
            }
        }

        if ($lost !== []) {
            // @infection-ignore-all
            $this->logger->warning('partitions wait for a leader', [
                'partitions' => array_map(\strval(...), $lost),
            ]);
        }

        if ($resets === []) {
            return;
        }

        // A request that runs out of time comes back here as a failed attempt.
        // So the next attempt has a pause and new metadata, and serves the other waiting partitions too.
        $offsets = $this->offsets->list($resets, $metadata, $cancellation, retryTimeouts: false);

        foreach ($resets as $tp) {
            $leader = $metadata->topics[$tp->topic]->partitions[$tp->partition]->leader ?? -1;
            \assert($leader >= 0);

            $this->subscription->seek($tp, $offsets[$tp]);
            $this->subscription->follow($tp, $leader);
            $this->startFetcher($leader);

            // @infection-ignore-all
            $this->logger->info('offset reset', [
                'topic' => $tp->topic,
                'partition' => $tp->partition,
                'offset' => $offsets[$tp],
                'reset' => $this->config->autoOffsetReset->name,
            ]);
        }
    }
}

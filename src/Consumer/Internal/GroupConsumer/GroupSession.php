<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Pipeline;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\Internal\AssignmentConsumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;
use function Amp\async;
use function Amp\delay;

/**
 * The lifetime of a single group generation: resolve the start offsets for the
 * assigned partitions, drive the fetch core, and heartbeat the coordinator to
 * keep the membership alive. It ends when the coordinator asks for a rebalance
 * (surfaced through a heartbeat error) or when the consumer is closed.
 *
 * The heartbeat loop and the fetch-forward loop share one {@see DeferredCancellation}:
 * when the heartbeat stops — for any reason — it cancels that token, which tears
 * down the fetch loop and hands control back to the join/rebalance driver.
 *
 * @internal
 */
final readonly class GroupSession
{
    public function __construct(
        private AssignmentConsumer $consumer,
        private Coordinator $coordinator,
        private OffsetResolver $offsets,
        private Consumer\GroupConfig $group,
        private Membership $membership,
        private ContextLogger $logger,
    ) {}

    /**
     * @param Pipeline\Queue<non-empty-list<Consumer\Record>> $queue
     * @throws Exception
     */
    public function run(Pipeline\Queue $queue, Cancellation $cancellation): void
    {
        $offsets = $this->offsets->resolve(
            $this->membership->assignments,
            $cancellation,
        );

        // @infection-ignore-all
        $this->logger->info('group started', [
            'generationId' => $this->membership->generationId,
            'memberId' => $this->membership->memberId,
            'offsets' => array_map(
                static fn(TopicPartition $tp) => [(string) $tp => $offsets[$tp]],
                $this->membership->assignments,
            ),
        ]);

        $rebalance = new DeferredCancellation();
        $cancel = new CompositeCancellation(
            $cancellation,
            $rebalance->getCancellation(),
        );

        $heartbeat = async($this->heartbeat(...), $rebalance, $cancel);

        try {
            if (\count($offsets) > 0) {
                $this->consumer->start($offsets, $cancel);

                foreach ($this->consumer->consume(Consumer\ConsumeMode::Stream, $cancel) as $batch) {
                    $queue->push($batch);
                }
            }

            // The fetch loop only unblocks once $cancel is cancelled, i.e. once the
            // heartbeat has ended the generation. Awaiting it here re-raises a fatal
            // heartbeat error; for an empty assignment this is the only wait.
            $heartbeat->await();
        } catch (CancelledException) {
            // the outer cancellation fired.
        } finally {
            $rebalance->cancel();
            $this->consumer->stop();
            $heartbeat->ignore();
        }
    }

    /**
     * @throws Exception
     * @throws CancelledException
     */
    private function heartbeat(DeferredCancellation $rebalance, Cancellation $cancellation): void
    {
        $interval = $this->group->heartbeatInterval->toSeconds(3);

        try {
            while (true) {
                delay($interval, cancellation: $cancellation);

                $response = $this->coordinator->request(
                    new Generated\HeartbeatRequest(
                        groupId: $this->group->groupId,
                        generationId: $this->membership->generationId,
                        memberId: $this->membership->memberId,
                        groupInstanceId: $this->group->groupInstanceId,
                    ),
                    $cancellation,
                );

                $errors = Errors::for($response);

                if ($errors->empty()) {
                    continue;
                }

                // The coordinator wants us to rejoin: a normal, expected outcome.
                if ($errors->has(ErrorCode::RebalanceInProgress, ErrorCode::IllegalGeneration, ErrorCode::UnknownMemberID)) {
                    // @infection-ignore-all
                    $this->logger->info('rebalance signalled by heartbeat', [
                        'error' => (string) $errors,
                        'generationId' => $this->membership->generationId,
                    ]);

                    return;
                }

                throw new Exception\ProtocolError($errors);
            }
        } finally {
            $rebalance->cancel();
        }
    }
}

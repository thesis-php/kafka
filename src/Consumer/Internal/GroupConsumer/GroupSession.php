<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\Internal\AssignmentConsumer;
use Thesis\Kafka\Consumer\Internal\Buffer;
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
 * The fetch puts its batches straight into the buffer of the group consumer.
 * When the heartbeat stops — for any reason — the generation ends: the fetch
 * stops, and control goes back to the join/rebalance driver. A fatal error of
 * the fetch ends the generation too.
 *
 * @internal
 */
final class GroupSession
{
    private readonly DeferredCancellation $ended;

    private ?\Throwable $failure = null;

    public function __construct(
        private readonly AssignmentConsumer $consumer,
        private readonly Coordinator $coordinator,
        private readonly OffsetResolver $offsets,
        private readonly Consumer\GroupConfig $group,
        private readonly Membership $membership,
        private readonly ContextLogger $logger,
    ) {
        $this->ended = new DeferredCancellation();
    }

    /**
     * Whether the generation of this session still runs. A batch of an ended
     * generation must not reach the user: its partition can belong to another
     * member now, and the owner fetches it again from the committed offset.
     */
    public function live(): bool
    {
        return !$this->ended->isCancelled();
    }

    /**
     * @param Buffer<Delivery> $buffer
     * @throws Exception
     * @throws CancelledException a time limit of a request
     */
    public function run(Buffer $buffer, Cancellation $cancellation): void
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

        $cancel = new CompositeCancellation(
            $cancellation,
            $this->ended->getCancellation(),
        );

        $heartbeat = async($this->heartbeat(...), $cancel);

        try {
            try {
                if (\count($offsets) > 0) {
                    $this->consumer->start(
                        $offsets,
                        // A full buffer waits for the user. The end of the generation stops
                        // the fetch and this wait, so a slow user cannot delay the rebalance.
                        deliver: function (array $records, Cancellation $cancellation) use ($buffer): void {
                            $buffer->push(new Delivery($this, $records), $cancellation);
                        },
                        failed: $this->fail(...),
                        cancellation: $cancel,
                    );
                }
            } catch (CancelledException $e) {
                // Without the end of the generation it is a time limit of a request.
                if (!$cancel->isRequested()) {
                    throw $e;
                }
            }

            // The heartbeat runs for the whole generation. Awaiting it here re-raises
            // a fatal heartbeat error.
            $heartbeat->await();
        } catch (CancelledException $e) {
            // close() and a fatal error of the fetch stop the heartbeat this way.
            // Any other cancellation is a time limit of a request.
            if (!$cancellation->isRequested() && $this->failure === null) {
                throw $e;
            }
        } finally {
            $this->ended->cancel();
            $this->consumer->stop();
            $heartbeat->ignore();
        }

        if ($this->failure !== null) {
            throw $this->failure;
        }
    }

    /**
     * The fetch has stopped with an error that cannot be fixed.
     */
    private function fail(\Throwable $error): void
    {
        $this->failure = $error;
        $this->ended->cancel();
    }

    /**
     * @throws Exception
     * @throws CancelledException
     */
    private function heartbeat(Cancellation $cancellation): void
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
            $this->ended->cancel();
        }
    }
}

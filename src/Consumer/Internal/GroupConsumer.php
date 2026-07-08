<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\TimeoutCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\Protocol\DataSerializer;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final class GroupConsumer implements Consumer
{
    private readonly AssignmentConsumer $consumer;

    private readonly GroupConsumer\Coordinator $coordinator;

    private readonly GroupConsumer\JoinController $joinController;

    private readonly GroupConsumer\OffsetResolver $offsets;

    private readonly GroupConsumer\OffsetCommitter $committer;

    /**
     * The membership of the generation currently running. Read by {@see commit()};
     * a commit carrying a stale generation is rejected by the coordinator.
     */
    private ?GroupConsumer\Membership $membership = null;

    /** @var ?Future<*> */
    private ?Future $loop = null;

    private GroupConsumer\ConsumerState $state = GroupConsumer\ConsumerState::Idle;

    private GroupConsumer\ConsumeState $consume = GroupConsumer\ConsumeState::Idle;

    /** @var Pipeline\Queue<non-empty-list<Consumer\Record>> */
    private readonly Pipeline\Queue $queue;

    /** @var Pipeline\ConcurrentIterator<non-empty-list<Consumer\Record>> */
    private readonly Pipeline\ConcurrentIterator $iterator;

    private readonly DataSerializer $data;

    private readonly DeferredCancellation $cancellation;

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly Consumer\Config $config,
        private readonly Consumer\GroupConfig $group,
        private readonly TimeSpan $requestTimeout,
        private readonly array $topics,
        private readonly ContextLogger $logger,
    ) {
        $this->data = new DataSerializer();
        $this->queue = new Pipeline\Queue($config->prefetchBufferSize);
        $this->iterator = $this->queue->iterate();
        $this->cancellation = new DeferredCancellation();
        $this->coordinator = new GroupConsumer\Coordinator(
            groupId: $group->groupId,
            cluster: $cluster,
            logger: $logger,
        );

        $this->consumer = new AssignmentConsumer(
            cluster: $this->cluster,
            config: $this->config,
            logger: $this->logger,
        );

        $this->joinController = new GroupConsumer\JoinController(
            cluster: $this->cluster,
            coordinator: $this->coordinator,
            group: $this->group,
            topics: $this->topics,
            data: $this->data,
        );

        $this->offsets = new GroupConsumer\OffsetResolver(
            cluster: $this->cluster,
            coordinator: $this->coordinator,
            lister: new OffsetLister(
                cluster: $this->cluster,
                config: $this->config,
                requestTimeout: $this->requestTimeout,
            ),
            groupId: $this->group->groupId,
            autoOffsetReset: $this->config->autoOffsetReset,
        );

        $this->committer = new GroupConsumer\OffsetCommitter(
            cluster: $this->cluster,
            coordinator: $this->coordinator,
            group: $this->group,
        );
    }

    #[\Override]
    public function consume(
        Consumer\ConsumeMode $mode = Consumer\ConsumeMode::Stream,
        ?Cancellation $cancellation = null,
    ): iterable {
        if ($this->state === GroupConsumer\ConsumerState::Idle) {
            $this->state = GroupConsumer\ConsumerState::Running;
            $this->loop = async($this->run(...));
        } elseif ($this->state === GroupConsumer\ConsumerState::Closed) {
            throw new Exception\ConsumerClosed();
        }

        if ($this->consume === GroupConsumer\ConsumeState::Running) {
            throw new Exception\ConsumerBusy();
        }

        $this->consume = GroupConsumer\ConsumeState::Running;

        if ($cancellation === null && $mode === Consumer\ConsumeMode::Poll) {
            $cancellation = new TimeoutCancellation(
                $this->config->pollTimeout->toSeconds(3),
            );
        }

        $cancellation ??= new NullCancellation();

        return $this->createIterator($mode, $cancellation);
    }

    #[\Override]
    public function commit(
        array|Consumer\Record $records,
        Cancellation $cancellation = new NullCancellation(),
    ): void {
        $membership = $this->membership
            ?? throw new \LogicException('Cannot commit before the consumer has joined the group.');

        if (!\is_array($records)) {
            $records = [$records];
        }

        /** @var TopicPartition\Map<non-negative-int> $offsets */
        $offsets = new TopicPartition\Map();
        foreach ($records as $record) {
            $tp = new TopicPartition($record->topic, $record->partition);
            // Committed offset is the next one to read: last processed + 1.
            $next = $record->offset + 1;
            $offsets[$tp] = isset($offsets[$tp]) ? max($offsets[$tp], $next) : $next;
        }

        $this->committer->commit($offsets, $membership, $cancellation);
    }

    #[\Override]
    public function close(): void
    {
        if ($this->state !== GroupConsumer\ConsumerState::Running) {
            return;
        }

        $this->logger->info('consumer closing');

        $this->state = GroupConsumer\ConsumerState::Closed;
        $this->cancellation->cancel();
        $this->loop?->await();
        $this->cluster->close();
    }

    private function run(): void
    {
        $cancellation = $this->cancellation->getCancellation();
        $memberId = '';

        try {
            while ($this->state === GroupConsumer\ConsumerState::Running && !$this->cancellation->isCancelled()) {
                $membership = $this->joinController->join($memberId, $cancellation);
                // The coordinator-assigned id is our identity across rebalances.
                $memberId = $membership->memberId;
                $this->membership = $membership;

                $session = new GroupConsumer\GroupSession(
                    consumer: $this->consumer,
                    coordinator: $this->coordinator,
                    offsets: $this->offsets,
                    group: $this->group,
                    membership: $membership,
                    logger: $this->logger,
                );

                // Blocks for the whole generation; returns here to rejoin on rebalance.
                $session->run($this->queue, $cancellation);
            }

            $this->queue->complete();
        } catch (CancelledException) {
            $this->queue->complete(); // expected on close()
        } catch (\Throwable $e) {
            $this->queue->error($e);
        } finally {
            $this->consumer->stop();
            $this->leaveGroup($memberId);
        }
    }

    /**
     * Proactively leave the group so the coordinator rebalances the remaining
     * members immediately instead of waiting out our session timeout. Best
     * effort: a failed leave only costs that wait, so it never blocks close().
     * The run cancellation has already fired here, hence a fresh timeout; the
     * cluster connection is still open (close() drops it only after we return).
     * Static members keep their slot across restarts and don't leave.
     *
     * @param string $memberId the last generation's member id, '' if never joined
     */
    private function leaveGroup(string $memberId): void
    {
        if ($memberId === '' || $this->group->groupInstanceId !== null) {
            return;
        }

        try {
            $response = $this->coordinator->request(
                new Generated\LeaveGroupRequest(
                    groupId: $this->group->groupId,
                    memberId: $memberId, // v0-2
                    members: [            // v3+
                        new Generated\LeaveGroupRequest\MemberIdentity(
                            memberId: $memberId,
                            groupInstanceId: null,
                            reason: 'consumer closed',
                        ),
                    ],
                ),
                new TimeoutCancellation($this->requestTimeout->toSeconds(3)),
            );

            $errors = Errors::for($response);
            if (!$errors->empty()) {
                throw new Exception\ProtocolError($errors);
            }
        } catch (\Throwable $e) {
            $this->logger->warning('leave group failed', ['exception' => $e]);
        }
    }

    /**
     * @return iterable<non-empty-list<Consumer\Record>>
     */
    private function createIterator(
        Consumer\ConsumeMode $mode,
        Cancellation $cancellation,
    ): iterable {
        try {
            if (!$this->iterator->continue($cancellation)) {
                return;
            }

            yield $this->iterator->getValue();

            if ($mode === Consumer\ConsumeMode::Stream) {
                while ($this->iterator->continue($cancellation)) {
                    yield $this->iterator->getValue();
                }
            }
        } catch (CancelledException) {
            return;
        } finally {
            $this->consume = GroupConsumer\ConsumeState::Idle;
        }
    }
}

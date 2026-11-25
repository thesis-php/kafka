<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use Amp\NullCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\Retry;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * @internal
 */
final readonly class OffsetLister
{
    public function __construct(
        private Cluster $cluster,
        private Consumer\Config $config,
        private TimeSpan $requestTimeout,
        private ContextLogger $logger,
    ) {}

    /**
     * @param non-empty-list<TopicPartition> $partitions
     * @return TopicPartition\Map<non-negative-int>
     * @throws Exception
     */
    public function list(
        array $partitions,
        Cluster\Metadata $metadata,
        Cancellation $cancellation = new NullCancellation(),
    ): TopicPartition\Map {
        $byLeader = [];
        foreach ($partitions as $tp) {
            $topicMetadata = $metadata->topics[$tp->topic]
                ?? throw new Exception\UnknownTopic($tp->topic);

            $leader = $topicMetadata->partitions[$tp->partition]->leader ?? -1;
            if ($leader === -1) {
                throw new Exception\NoAvailablePartitionLeader($tp->topic, $tp->partition);
            }

            $byLeader[$leader][] = $tp;
        }

        $futures = [];
        foreach ($byLeader as $leaderId => $leaderPartitions) {
            $futures[$leaderId] = async(
                $this->listLeader(...),
                $leaderId,
                $leaderPartitions,
                $cancellation,
            );
        }

        /** @var list<TopicPartition\Map<non-negative-int>> $perLeader */
        $perLeader = Future\await($futures, $cancellation);

        /** @var TopicPartition\Map<non-negative-int> $result */
        $result = new TopicPartition\Map();
        foreach ($perLeader as $leaderResult) {
            foreach ($leaderResult as $tp) {
                $result[$tp] = $leaderResult[$tp];
            }
        }

        return $result;
    }

    /**
     * @param non-negative-int $leaderId
     * @param non-empty-list<TopicPartition> $partitions
     * @return TopicPartition\Map<non-negative-int>
     * @throws Exception
     * @throws CancelledException
     */
    private function listLeader(
        int $leaderId,
        array $partitions,
        Cancellation $cancellation,
    ): TopicPartition\Map {
        /** @var array<non-empty-string, list<Generated\ListOffsetsRequest\ListOffsetsTopic\ListOffsetsPartition>> $partitionsByTopic */
        $partitionsByTopic = [];

        foreach ($partitions as $tp) {
            $partitionsByTopic[$tp->topic][] = new Generated\ListOffsetsRequest\ListOffsetsTopic\ListOffsetsPartition(
                partitionIndex: $tp->partition,
                currentLeaderEpoch: -1,
                timestamp: $this->config->autoOffsetReset->value,
            );
        }

        $topics = [];
        foreach ($partitionsByTopic as $topic => $listPartitions) {
            $topics[] = new Generated\ListOffsetsRequest\ListOffsetsTopic(
                name: $topic,
                partitions: $listPartitions,
            );
        }

        $request = new Generated\ListOffsetsRequest(
            replicaId: -1, // -1 = consumer, not a replica
            isolationLevel: $this->config->isolationLevel->value,
            topics: $topics,
            timeoutMs: (int) $this->requestTimeout->toMilliseconds(3),
        );

        // The leader from the metadata may not know a new topic yet. It answers
        // with a retriable error, so the request is sent again.
        $response = $this->cluster->request(
            request: $request,
            cancellation: $cancellation,
            brokerId: $leaderId,
            retry: new Retry\Operation(
                static fn(Generated\ListOffsetsResponse $response): ErrorCode => (Errors::for($response)->errors[0] ?? null)?->code() ?? ErrorCode::None,
                // @infection-ignore-all
                logger: $this->logger->with([
                    'component' => 'retry',
                    'operation' => 'listOffsets',
                ]),
            ),
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new Exception\ProtocolError($errors);
        }

        /** @var TopicPartition\Map<non-negative-int> $result */
        $result = new TopicPartition\Map();

        foreach ($response->topics as $topicResponse) {
            foreach ($topicResponse->partitions as $partitionResponse) {
                $topic = $topicResponse->name;
                \assert($topic !== '');

                $partition = $partitionResponse->partitionIndex;
                \assert($partition >= 0);

                $tp = new TopicPartition(
                    $topic,
                    $partition,
                );

                $offset = $partitionResponse->offset;
                \assert($offset >= 0);

                $result[$tp] = $offset;
            }
        }

        return $result;
    }
}

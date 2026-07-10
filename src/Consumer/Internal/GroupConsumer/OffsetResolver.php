<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\NullCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\Internal\OffsetLister;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final readonly class OffsetResolver
{
    /**
     * @param non-empty-string $groupId
     */
    public function __construct(
        private Cluster $cluster,
        private Coordinator $coordinator,
        private OffsetLister $lister,
        private string $groupId,
        private Consumer\AutoOffsetReset $autoOffsetReset,
        private ContextLogger $logger,
    ) {}

    /**
     * @param list<TopicPartition> $assignment
     * @return TopicPartition\Map<non-negative-int>
     * @throws Exception
     */
    public function resolve(
        array $assignment,
        Cancellation $cancellation = new NullCancellation(),
    ): TopicPartition\Map {
        /** @var TopicPartition\Map<non-negative-int> $offsets */
        $offsets = new TopicPartition\Map();

        if ($assignment === []) {
            return $offsets;
        }

        $metadata = $this->cluster->refreshMetadata(
            array_map(static fn(TopicPartition $partition) => $partition->topic, $assignment),
            $cancellation,
        );

        $committed = $this->fetchCommitted(
            $assignment,
            $metadata,
            $cancellation,
        );

        /** @var list<TopicPartition> $uncommited */
        $uncommited = [];

        foreach ($assignment as $tp) {
            $offset = $committed[$tp] ?? -1;
            if ($offset < 0) {
                $uncommited[] = $tp;
            } else {
                $offsets[$tp] = $offset;
            }
        }

        if ($uncommited !== []) {
            // The group never committed these partitions. None forbids inventing
            // a starting position from the log, so fail instead of resetting.
            if ($this->autoOffsetReset === Consumer\AutoOffsetReset::None) {
                $this->logger->warning('no committed offset and reset is disabled', [
                    'partitions' => array_map(
                        \strval(...),
                        $uncommited,
                    ),
                ]);

                throw new Exception\NoOffsetForPartitions($uncommited);
            }

            $reset = $this->lister->list($uncommited, $metadata, $cancellation);
            foreach ($reset as $tp) {
                $offsets[$tp] = $reset[$tp];
            }

            $this->logger->notice('no committed offset, resetting', [
                'reset' => $this->autoOffsetReset->name,
                'partitions' => array_map(
                    static fn(TopicPartition $tp) => "{$tp} -> {$reset[$tp]}",
                    $uncommited,
                ),
            ]);
        }

        return $offsets;
    }

    /**
     * @param non-empty-list<TopicPartition> $assignment
     * @return TopicPartition\Map<int>
     * @throws Exception
     */
    private function fetchCommitted(
        array $assignment,
        Cluster\Metadata $metadata,
        Cancellation $cancellation,
    ): TopicPartition\Map {
        /** @var array<non-empty-string, list<non-negative-int>> $byTopic */
        $byTopic = [];
        foreach ($assignment as $tp) {
            $byTopic[$tp->topic][] = $tp->partition;
        }

        $legacyTopics = [];
        $groupTopics = [];
        foreach ($byTopic as $topic => $partitions) {
            $topicId = $metadata->topics[$topic]->topicId ?? throw new Exception\UnknownTopic($topic);

            $legacyTopics[] = new Generated\OffsetFetchRequest\OffsetFetchRequestTopic(
                name: $topic,
                partitionIndexes: $partitions,
            );

            $groupTopics[] = new Generated\OffsetFetchRequest\OffsetFetchRequestGroup\OffsetFetchRequestTopics(
                name: $topic,
                topicId: $topicId,
                partitionIndexes: $partitions,
            );
        }

        $response = $this->coordinator->request(
            new Generated\OffsetFetchRequest(
                groupId: $this->groupId,
                topics: $legacyTopics,
                groups: [
                    new Generated\OffsetFetchRequest\OffsetFetchRequestGroup(
                        groupId: $this->groupId,
                        memberId: null, // classic protocol, not KIP-848
                        memberEpoch: -1,
                        topics: $groupTopics,
                    ),
                ],
                requireStable: false,
            ),
            $cancellation,
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new Exception\ProtocolError($errors);
        }

        return self::extract($response, $metadata);
    }

    /**
     * @return TopicPartition\Map<int>
     */
    private static function extract(
        Generated\OffsetFetchResponse $response,
        Cluster\Metadata $metadata,
    ): TopicPartition\Map {
        $namesByTopicId = [];
        foreach ($metadata->topics as $name => $meta) {
            $namesByTopicId[$meta->topicId->toString()] = $name;
        }

        /** @var TopicPartition\Map<int> $committed */
        $committed = new TopicPartition\Map();

        foreach ($response->topics as $topic) {
            \assert($topic->name !== '');

            foreach ($topic->partitions as $partition) {
                \assert($partition->partitionIndex >= 0);

                $committed[new TopicPartition($topic->name, $partition->partitionIndex)] = $partition->committedOffset;
            }
        }

        foreach ($response->groups as $group) {
            foreach ($group->topics as $topic) {
                $name = $topic->name !== ''
                    ? $topic->name
                    : ($namesByTopicId[$topic->topicId->toString()] ?? throw new \UnexpectedValueException(
                        "OffsetFetch returned an unknown topic id '{$topic->topicId->toString()}'.",
                    ));

                foreach ($topic->partitions as $partition) {
                    \assert($partition->partitionIndex >= 0);

                    $committed[new TopicPartition($name, $partition->partitionIndex)] = $partition->committedOffset;
                }
            }
        }

        return $committed;
    }
}

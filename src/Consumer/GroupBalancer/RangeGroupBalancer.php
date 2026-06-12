<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\GroupBalancer;

use Thesis\Kafka\Consumer\ClassicGroupBalancer;
use Thesis\Kafka\Consumer\RebalanceProtocol;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @api
 */
final readonly class RangeGroupBalancer implements ClassicGroupBalancer
{
    #[\Override]
    public function name(): string
    {
        return 'range';
    }

    #[\Override]
    public function protocol(): RebalanceProtocol
    {
        return RebalanceProtocol::Eager;
    }

    #[\Override]
    public function memberMetadata(
        array $topics,
        array $ownedPartitions,
        int $generationId,
    ): Generated\ConsumerProtocolSubscription {
        return new Generated\ConsumerProtocolSubscription(
            topics: $topics,
            userData: null,
            ownedPartitions: [],
            generationId: $generationId,
            rackId: null,
        );
    }

    #[\Override]
    public function assign(array $members, array $partitionCounts): array
    {
        // Range works per topic, independently. To slice a topic's partitions
        // we need its subscribers, but members are keyed the other way (member
        // → topics). Invert into topic → subscribers.
        $subscribersByTopic = [];

        foreach ($members as $member) {
            foreach ($member->topics as $topic) {
                $subscribersByTopic[$topic][] = $member->memberId;
            }
        }

        // Seed every member with an empty assignment so a member that ends up
        // with no partitions still has a key — SyncGroup sends each member its
        // (possibly empty) assignment, and a missing key would mean no message.
        $result = [];
        foreach ($members as $member) {
            $result[$member->memberId] = [];
        }

        foreach ($subscribersByTopic as $topic => $subscribers) {
            // Sort subscribers for determinism. Only the leader runs this, but
            // the result must not depend on the order members happened to join —
            // sorting by id makes the slicing reproducible.
            sort($subscribers);

            $partitionCount = $partitionCounts[$topic] ?? 0;
            $subscriberCount = \count($subscribers);

            // Base share each subscriber gets, plus the leftover that doesn't
            // divide evenly. The first $remainder subscribers take one extra
            // each. E.g. 7 partitions / 2 subscribers → base 3, remainder 1 →
            // first gets 4, second gets 3.
            $perSubscriber = intdiv($partitionCount, $subscriberCount);
            $remainder = $partitionCount % $subscriberCount;

            // Walk partitions 0..N-1 with a running cursor, handing each
            // subscriber a contiguous block. Contiguity is what makes this
            // "range": subscriber 0 gets [0..k), subscriber 1 continues from k,
            // and so on.
            $partition = 0;
            foreach ($subscribers as $index => $memberId) {
                // First $remainder subscribers get the +1 leftover partition.
                $count = $perSubscriber + ($index < $remainder ? 1 : 0);
                for ($i = 0; $i < $count; ++$i) {
                    $result[$memberId][] = new TopicPartition($topic, $partition);
                    ++$partition;
                }
            }
        }

        return $result;
    }
}

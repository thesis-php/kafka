<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\NullCancellation;
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Consumer\GroupMember;
use Thesis\Kafka\Consumer\RebalanceProtocol;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\Protocol\DataSerializer;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final readonly class JoinController
{
    private const string PROTOCOL_TYPE = 'consumer';

    /**
     * @param non-empty-list<non-empty-string> $topics
     */
    public function __construct(
        private Cluster $cluster,
        private Coordinator $coordinator,
        private Consumer\GroupConfig $group,
        private array $topics,
        private ContextLogger $logger,
        private DataSerializer $data = new DataSerializer(),
    ) {}

    /**
     * @throws CancelledException
     * @throws Exception
     */
    public function join(
        string $memberId = '',
        Cancellation $cancellation = new NullCancellation(),
    ): Membership {
        while (true) {
            $join = $this->doJoin($memberId, $cancellation);

            $errors = Errors::for($join);

            if ($errors->has(ErrorCode::MemberIDRequired, ErrorCode::RebalanceInProgress)) {
                $memberId = $join->memberId;
                continue;
            }

            if ($errors->has(ErrorCode::UnknownMemberID)) {
                $memberId = '';
                continue;
            }

            if (!$errors->empty()) {
                throw new Exception\ProtocolError($errors);
            }

            $assignments = [];

            if ($join->leader === $memberId && !$join->skipAssignment) {
                $assignments = $this->balance($join, $cancellation);
            }

            $sync = $this->doSync($join, $memberId, $assignments, $cancellation);

            $errors = Errors::for($sync);

            if ($errors->has(ErrorCode::RebalanceInProgress, ErrorCode::IllegalGeneration, ErrorCode::UnknownMemberID)) {
                if ($errors->has(ErrorCode::UnknownMemberID)) {
                    $memberId = '';
                }

                continue;
            }

            if (!$errors->empty()) {
                throw new Exception\ProtocolError($errors);
            }

            $isLeader = $join->leader === $memberId;

            // @infection-ignore-all
            $this->logger->info('joined group', [
                'generationId' => $join->generationId,
                'memberId' => $memberId,
                'protocol' => $sync->protocolName,
                'role' => $isLeader ? 'leader' : 'follower',
                'members' => $isLeader ? \count($join->members) : null,
            ]);

            return new Membership(
                memberId: $memberId,
                generationId: $join->generationId,
                protocolName: $sync->protocolName,
                assignments: $this->decodeAssignment($sync->assignment),
            );
        }
    }

    /**
     * @throws CancelledException
     * @throws Exception
     */
    private function doJoin(string $memberId, Cancellation $cancellation): Generated\JoinGroupResponse
    {
        $protocols = [];
        foreach ($this->group->balancers as $balancer) {
            $subscription = new Generated\ConsumerProtocolSubscription(
                topics: $this->topics,
                userData: null,
                ownedPartitions: [],
                generationId: -1,
                rackId: null,
            );

            $protocols[] = new Generated\JoinGroupRequest\JoinGroupRequestProtocol(
                name: $balancer->name(),
                metadata: $this->data->serialize(
                    $subscription,
                    match ($balancer->protocol()) {
                        RebalanceProtocol::Eager => 0,
                        RebalanceProtocol::Cooperative => 1,
                    },
                ),
            );
        }

        $request = new Generated\JoinGroupRequest(
            groupId: $this->group->groupId,
            sessionTimeoutMs: (int) $this->group->sessionTimeout->toMilliseconds(3),
            rebalanceTimeoutMs: (int) $this->group->rebalanceTimeout->toMilliseconds(3),
            memberId: $memberId,
            groupInstanceId: $this->group->groupInstanceId,
            protocolType: self::PROTOCOL_TYPE,
            protocols: $protocols,
            reason: null,
        );

        return $this->coordinator->request(
            $request,
            $cancellation,
        );
    }

    /**
     * @param list<Generated\SyncGroupRequest\SyncGroupRequestAssignment> $assignments
     * @throws Exception
     * @throws CancelledException
     */
    private function doSync(
        Generated\JoinGroupResponse $joined,
        string $memberId,
        array $assignments,
        Cancellation $cancellation,
    ): Generated\SyncGroupResponse {
        $request = new Generated\SyncGroupRequest(
            groupId: $this->group->groupId,
            generationId: $joined->generationId,
            memberId: $memberId,
            groupInstanceId: $this->group->groupInstanceId,
            protocolType: self::PROTOCOL_TYPE,
            protocolName: $joined->protocolName,
            assignments: $assignments,
        );

        return $this->coordinator->request(
            $request,
            $cancellation,
        );
    }

    /**
     * @return list<Generated\SyncGroupRequest\SyncGroupRequestAssignment>
     */
    private function balance(Generated\JoinGroupResponse $join, Cancellation $cancellation): array
    {
        $balancer = $this->balancerFor($join->protocolName);

        $members = [];
        $subscribedTopics = [];
        foreach ($join->members as $member) {
            $subscription = $this->data->deserialize(Generated\ConsumerProtocolSubscription::class, $member->metadata);

            $ownedPartitions = [];

            foreach ($subscription->ownedPartitions as $tp) {
                $ownedPartitions[$tp->topic] = $tp->partitions;
            }

            $members[] = new GroupMember(
                memberId: $member->memberId,
                topics: $subscription->topics,
                ownedPartitions: $ownedPartitions,
                userData: $subscription->userData,
            );

            $subscribedTopics = [
                ...$subscribedTopics,
                ...$subscription->topics,
            ];
        }

        $metadata = $this->cluster->refreshMetadata($subscribedTopics, $cancellation);

        $partitionCounts = [];

        foreach ($metadata->topics as $topic => $meta) {
            $partitionCounts[$topic] = $meta->partitions();
        }

        \assert($members !== []);

        $planned = $balancer->assign($members, $partitionCounts);

        // @infection-ignore-all
        $this->logger->debug('computed assignment', [
            'balancer' => $balancer->name(),
            'plan' => array_map(
                static fn(array $partitions) => array_map(
                    \strval(...),
                    $partitions,
                ),
                $planned,
            ),
        ]);

        $version = match ($balancer->protocol()) {
            RebalanceProtocol::Eager => 0,
            RebalanceProtocol::Cooperative => 1,
        };

        $assignments = [];

        foreach ($planned as $memberId => $partitions) {
            $assignment = new Generated\ConsumerProtocolAssignment(
                assignedPartitions: $this->groupByTopic($partitions),
                userData: null,
            );

            $assignments[] = new Generated\SyncGroupRequest\SyncGroupRequestAssignment(
                memberId: $memberId,
                assignment: $this->data->serialize($assignment, $version),
            );
        }

        return $assignments;
    }

    private function balancerFor(?string $protocolName): Consumer\ClassicGroupBalancer
    {
        foreach ($this->group->balancers as $balancer) {
            if ($balancer->name() === $protocolName) {
                return $balancer;
            }
        }

        throw new \UnexpectedValueException(
            "Coordinator selected unknown balancer protocol: '{$protocolName}'.",
        );
    }

    /**
     * @param list<TopicPartition> $partitions
     * @return list<Generated\ConsumerProtocolAssignment\TopicPartition>
     */
    private function groupByTopic(array $partitions): array
    {
        $byTopic = [];
        foreach ($partitions as $tp) {
            $byTopic[$tp->topic][] = $tp->partition;
        }

        $result = [];
        foreach ($byTopic as $topic => $ids) {
            $result[] = new Generated\ConsumerProtocolAssignment\TopicPartition(
                topic: $topic,
                partitions: $ids,
            );
        }

        return $result;
    }

    /**
     * @return list<TopicPartition>
     */
    private function decodeAssignment(string $bytes): array
    {
        $assignment = $this->data->deserialize(Generated\ConsumerProtocolAssignment::class, $bytes);

        $result = [];
        foreach ($assignment->assignedPartitions as $tp) {
            $topic = $tp->topic;
            \assert($topic !== '');

            foreach ($tp->partitions as $partition) {
                \assert($partition >= 0);

                $result[] = new TopicPartition($topic, $partition);
            }
        }

        return $result;
    }
}

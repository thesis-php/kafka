<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol;

/**
 * @api
 * @template-implements \IteratorAggregate<array-key, Error>
 */
final readonly class Errors implements
    \IteratorAggregate,
    \Countable
{
    /**
     * @param list<Error> $errors
     */
    public function __construct(
        public array $errors = [],
    ) {}

    public static function for(Generated\Response $response): self
    {
        $errors = match (true) {
            $response instanceof Generated\MetadataResponse => self::fromMetadata($response),
            $response instanceof Generated\CreateTopicsResponse => self::fromCreateTopics($response),
            $response instanceof Generated\DeleteTopicsResponse => self::fromDeleteTopics($response),
            $response instanceof Generated\CreatePartitionsResponse => self::fromCreatePartitions($response),
            $response instanceof Generated\ProduceResponse => self::fromProduce($response),
            $response instanceof Generated\FetchResponse => self::fromFetch($response),
            $response instanceof Generated\ListOffsetsResponse => self::fromListOffsets($response),
            $response instanceof Generated\OffsetCommitResponse => self::fromOffsetCommit($response),
            $response instanceof Generated\OffsetFetchResponse => self::fromOffsetFetch($response),
            $response instanceof Generated\OffsetDeleteResponse => self::fromOffsetDelete($response),
            $response instanceof Generated\AlterPartitionReassignmentsResponse => self::fromAlterPartitionReassignments($response),
            $response instanceof Generated\ListPartitionReassignmentsResponse => self::fromTopLevel($response->errorCode, $response->errorMessage),
            $response instanceof Generated\ElectLeadersResponse => self::fromElectLeaders($response),
            $response instanceof Generated\JoinGroupResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\SyncGroupResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\HeartbeatResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\LeaveGroupResponse => self::fromLeaveGroup($response),
            $response instanceof Generated\DescribeGroupsResponse => self::fromDescribeGroups($response),
            $response instanceof Generated\ListGroupsResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\DeleteGroupsResponse => self::fromDeleteGroups($response),
            $response instanceof Generated\FindCoordinatorResponse => self::fromTopLevel($response->errorCode, $response->errorMessage),
            $response instanceof Generated\DescribeClusterResponse => self::fromTopLevel($response->errorCode, $response->errorMessage),
            $response instanceof Generated\ApiVersionsResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\SaslHandshakeResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\SaslAuthenticateResponse => self::fromTopLevel($response->errorCode, $response->errorMessage),
            $response instanceof Generated\InitProducerIdResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\EndTxnResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\AddPartitionsToTxnResponse => self::fromAddPartitionsToTxn($response),
            $response instanceof Generated\AddOffsetsToTxnResponse => self::fromTopLevel($response->errorCode),
            $response instanceof Generated\TxnOffsetCommitResponse => self::fromTxnOffsetCommit($response),
            $response instanceof Generated\AlterConfigsResponse => self::fromAlterConfigs($response),
            $response instanceof Generated\IncrementalAlterConfigsResponse => self::fromIncrementalAlterConfigs($response),
            $response instanceof Generated\DescribeConfigsResponse => self::fromDescribeConfigs($response),
            $response instanceof Generated\CreateAclsResponse => self::fromCreateAcls($response),
            $response instanceof Generated\DeleteAclsResponse => self::fromDeleteAcls($response),
            $response instanceof Generated\DescribeAclsResponse => self::fromTopLevel($response->errorCode, $response->errorMessage),
            $response instanceof Generated\DescribeProducersResponse => self::fromDescribeProducers($response),
            $response instanceof Generated\DescribeTransactionsResponse => self::fromDescribeTransactions($response),
            $response instanceof Generated\ListTransactionsResponse => self::fromTopLevel($response->errorCode),
            default => [],
        };

        return new self($errors);
    }

    public function empty(): bool
    {
        return $this->errors === [];
    }

    public function has(ErrorCode $code): bool
    {
        return array_any($this->errors, static fn(Error $error) => $error->code() === $code);
    }

    /**
     * @return list<Error>
     */
    public function ofCode(ErrorCode $code): array
    {
        return \array_values(\array_filter(
            $this->errors,
            static fn(Error $error) => $error->code() === $code,
        ));
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->errors);
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromTopLevel(int $errorCode, ?string $errorMessage = null): array
    {
        $errors = [];

        $code = self::code($errorCode);
        if ($code !== ErrorCode::None) {
            $errors[] = new Error\TopLevelError($code, $errorMessage);
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromMetadata(Generated\MetadataResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            $code = self::code($topic->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\TopicError($topic->name ?? '<unknown>', $code);
            }

            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name ?? '<unknown>',
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromCreateTopics(Generated\CreateTopicsResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            $code = self::code($topic->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\TopicError(
                    topic: $topic->name,
                    code: $code,
                    message: $topic->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDeleteTopics(Generated\DeleteTopicsResponse $response): array
    {
        $errors = [];

        foreach ($response->responses as $topic) {
            $code = self::code($topic->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\TopicError(
                    topic: $topic->name ?? '<unknown>',
                    code: $code,
                    message: $topic->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromCreatePartitions(Generated\CreatePartitionsResponse $response): array
    {
        $errors = [];

        foreach ($response->results as $topic) {
            $code = self::code($topic->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\TopicError(
                    topic: $topic->name,
                    code: $code,
                    message: $topic->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromProduce(Generated\ProduceResponse $response): array
    {
        $errors = [];

        foreach ($response->responses as $topic) {
            foreach ($topic->partitionResponses as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->index,
                        code: $code,
                        message: $partition->errorMessage,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromFetch(Generated\FetchResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->responses as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->topic,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromListOffsets(Generated\ListOffsetsResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromOffsetCommit(Generated\OffsetCommitResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromOffsetFetch(Generated\OffsetFetchResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        foreach ($response->groups as $group) {
            $code = self::code($group->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\GroupError($group->groupId, $code);
            }

            foreach ($group->topics as $topic) {
                foreach ($topic->partitions as $partition) {
                    $code = self::code($partition->errorCode);
                    if ($code !== ErrorCode::None) {
                        $errors[] = new Error\PartitionError(
                            topic: $topic->name,
                            partition: $partition->partitionIndex,
                            code: $code,
                        );
                    }
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromOffsetDelete(Generated\OffsetDeleteResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromAlterPartitionReassignments(Generated\AlterPartitionReassignmentsResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode, $response->errorMessage);

        foreach ($response->responses as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                        message: $partition->errorMessage,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromElectLeaders(Generated\ElectLeadersResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->replicaElectionResults as $topic) {
            foreach ($topic->partitionResult as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->topic,
                        partition: $partition->partitionId,
                        code: $code,
                        message: $partition->errorMessage,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromLeaveGroup(Generated\LeaveGroupResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->members as $member) {
            $code = self::code($member->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\MemberError(
                    groupId: '<unknown>',
                    memberId: $member->memberId,
                    code: $code,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDescribeGroups(Generated\DescribeGroupsResponse $response): array
    {
        $errors = [];

        foreach ($response->groups as $group) {
            $code = self::code($group->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\GroupError(
                    groupId: $group->groupId,
                    code: $code,
                    message: $group->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDeleteGroups(Generated\DeleteGroupsResponse $response): array
    {
        $errors = [];

        foreach ($response->results as $group) {
            $code = self::code($group->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\GroupError($group->groupId, $code);
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromAddPartitionsToTxn(Generated\AddPartitionsToTxnResponse $response): array
    {
        $errors = self::fromTopLevel($response->errorCode);

        foreach ($response->resultsByTransaction as $transaction) {
            foreach ($transaction->topicResults as $topic) {
                foreach ($topic->resultsByPartition as $partition) {
                    $code = self::code($partition->partitionErrorCode);
                    if ($code !== ErrorCode::None) {
                        $errors[] = new Error\PartitionError(
                            topic: $topic->name,
                            partition: $partition->partitionIndex,
                            code: $code,
                        );
                    }
                }
            }
        }

        foreach ($response->resultsByTopicV3AndBelow as $topic) {
            foreach ($topic->resultsByPartition as $partition) {
                $code = self::code($partition->partitionErrorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromTxnOffsetCommit(Generated\TxnOffsetCommitResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromAlterConfigs(Generated\AlterConfigsResponse $response): array
    {
        $errors = [];

        foreach ($response->responses as $resource) {
            $code = self::code($resource->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\ConfigResourceError(
                    resourceType: $resource->resourceType,
                    resourceName: $resource->resourceName,
                    code: $code,
                    message: $resource->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromIncrementalAlterConfigs(Generated\IncrementalAlterConfigsResponse $response): array
    {
        $errors = [];

        foreach ($response->responses as $resource) {
            $code = self::code($resource->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\ConfigResourceError(
                    resourceType: $resource->resourceType,
                    resourceName: $resource->resourceName,
                    code: $code,
                    message: $resource->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDescribeConfigs(Generated\DescribeConfigsResponse $response): array
    {
        $errors = [];

        foreach ($response->results as $resource) {
            $code = self::code($resource->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\ConfigResourceError(
                    resourceType: $resource->resourceType,
                    resourceName: $resource->resourceName,
                    code: $code,
                    message: $resource->errorMessage,
                );
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromCreateAcls(Generated\CreateAclsResponse $response): array
    {
        $errors = [];

        foreach ($response->results as $result) {
            $code = self::code($result->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\AclError($code, $result->errorMessage);
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDeleteAcls(Generated\DeleteAclsResponse $response): array
    {
        $errors = [];

        foreach ($response->filterResults as $result) {
            $code = self::code($result->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\AclError($code, $result->errorMessage);
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDescribeProducers(Generated\DescribeProducersResponse $response): array
    {
        $errors = [];

        foreach ($response->topics as $topic) {
            foreach ($topic->partitions as $partition) {
                $code = self::code($partition->errorCode);
                if ($code !== ErrorCode::None) {
                    $errors[] = new Error\PartitionError(
                        topic: $topic->name,
                        partition: $partition->partitionIndex,
                        code: $code,
                        message: $partition->errorMessage,
                    );
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<Error>
     */
    private static function fromDescribeTransactions(Generated\DescribeTransactionsResponse $response): array
    {
        $errors = [];

        foreach ($response->transactionStates as $transaction) {
            $code = self::code($transaction->errorCode);
            if ($code !== ErrorCode::None) {
                $errors[] = new Error\TransactionError($transaction->transactionalId, $code);
            }
        }

        return $errors;
    }

    private static function code(int $value): ErrorCode
    {
        return $value === 0 ? ErrorCode::None : ErrorCode::tryFrom($value) ?? ErrorCode::UnknownServerError;
    }
}

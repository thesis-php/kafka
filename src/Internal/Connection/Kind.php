<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 */
enum Kind
{
    case Produce;
    case Fetch;
    case Group;
    case Slow;
    case Regular;

    /**
     * @param Generated\Request<*> $request
     */
    public static function for(Generated\Request $request): self
    {
        return match (true) {
            $request instanceof Generated\ProduceRequest => self::Produce,
            $request instanceof Generated\FetchRequest => self::Fetch,
            $request instanceof Generated\JoinGroupRequest,
            $request instanceof Generated\SyncGroupRequest => self::Group,
            $request instanceof Generated\CreateTopicsRequest,
            $request instanceof Generated\DeleteTopicsRequest,
            $request instanceof Generated\DeleteRecordsRequest,
            $request instanceof Generated\CreatePartitionsRequest,
            $request instanceof Generated\ElectLeadersRequest,
            $request instanceof Generated\AlterPartitionReassignmentsRequest,
            $request instanceof Generated\ListPartitionReassignmentsRequest,
            $request instanceof Generated\UpdateFeaturesRequest => self::Slow,
            default => self::Regular,
        };
    }
}

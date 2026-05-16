<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

/**
 * @internal
 */
enum PartitionRole
{
    case Leader;
    case InSyncReplica;
    case OutOfSyncReplica;
    case NotReplica;
}

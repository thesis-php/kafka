<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

/**
 * @internal
 */
enum TopicKind
{
    case User;
    case Internal;
}

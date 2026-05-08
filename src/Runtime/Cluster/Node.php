<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Cluster;

use Thesis\Kafka\Config\Endpoint;

/**
 * @internal
 */
final readonly class Node
{
    public function __construct(
        public int $id,
        public Endpoint $endpoint,
    ) {}
}

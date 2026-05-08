<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Cluster;

use Thesis\Kafka\Config\Endpoint;
use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 * @template-implements \IteratorAggregate<int, Node>
 */
final class MetadataList implements
    \IteratorAggregate,
    \Countable
{
    /**
     * @param array<int, Node> $nodes
     */
    public function __construct(
        public private(set) int $controllerId,
        public private(set) array $nodes = [],
    ) {}

    public function refresh(Generated\MetadataResponse $response): void
    {
        $this->controllerId = $response->controllerId;

        foreach ($response->brokers as $broker) {
            $this->nodes[$broker->nodeId] = new Node(
                id: $broker->nodeId,
                endpoint: new Endpoint(
                    host: $broker->host, // @phpstan-ignore argument.type
                    port: $broker->port, // @phpstan-ignore argument.type
                ),
            );
        }
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->nodes;
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->nodes);
    }
}

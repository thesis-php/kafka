<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster\Metadata;

use Thesis\Kafka\Config\Endpoint;

/**
 * @internal
 */
final readonly class BrokerMetadata
{
    /**
     * @param ?string $rack null when the broker doesn't advertise a rack
     */
    public function __construct(
        public int $id,
        public Endpoint $endpoint,
        public ?string $rack = null,
    ) {}
}

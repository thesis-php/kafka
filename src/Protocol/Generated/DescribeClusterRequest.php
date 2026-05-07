<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeClusterResponse>
 */
final readonly class DescribeClusterRequest implements Request
{
    /**
     * @param bool $includeClusterAuthorizedOperations Whether to include cluster authorized operations.
     * @param int $endpointType The endpoint type to describe. 1=brokers, 2=controllers.
     * @param bool $includeFencedBrokers Whether to include fenced brokers when listing brokers.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public bool $includeClusterAuthorizedOperations,
        public int $endpointType,
        public bool $includeFencedBrokers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeCluster;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeClusterResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeBool($this->includeClusterAuthorizedOperations);
        if ($version >= 1) {
            $writer->writeInt8($this->endpointType);
        }
        if ($version >= 2) {
            $writer->writeBool($this->includeFencedBrokers);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterPartitionResponse>
 */
final readonly class AlterPartitionRequest implements Request
{
    /**
     * @param int $brokerId The ID of the requesting broker.
     * @param int $brokerEpoch The epoch of the requesting broker.
     * @param list<AlterPartitionRequest\TopicData> $topics The topics to alter ISRs for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public int $brokerEpoch,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterPartition;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterPartitionResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeInt64($this->brokerEpoch);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

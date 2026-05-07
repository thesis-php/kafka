<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<CreatePartitionsResponse>
 */
final readonly class CreatePartitionsRequest implements Request
{
    /**
     * @param list<CreatePartitionsRequest\CreatePartitionsTopic> $topics Each topic that we want to create new partitions inside.
     * @param int $timeoutMs The time in ms to wait for the partitions to be created.
     * @param bool $validateOnly If true, then validate the request, but don't actually increase the number of partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topics,
        public int $timeoutMs,
        public bool $validateOnly,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::CreatePartitions;
    }

    #[\Override]
    public function responseType(): string
    {
        return CreatePartitionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->timeoutMs);
        $writer->writeBool($this->validateOnly);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AssignReplicasToDirsResponse>
 */
final readonly class AssignReplicasToDirsRequest implements Request
{
    /**
     * @param int $brokerId The ID of the requesting broker.
     * @param int $brokerEpoch The epoch of the requesting broker.
     * @param list<AssignReplicasToDirsRequest\DirectoryData> $directories The directories to which replicas should be assigned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public int $brokerEpoch,
        public array $directories,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AssignReplicasToDirs;
    }

    #[\Override]
    public function responseType(): string
    {
        return AssignReplicasToDirsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeInt64($this->brokerEpoch);
        $writer->writeCompactArrayLength(\count($this->directories));
        foreach ($this->directories as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

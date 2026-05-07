<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ReplicaState implements Encodable, Decodable
{
    /**
     * @param int $replicaId The replica ID of the follower, or -1 if this request is from a consumer.
     * @param int $replicaEpoch The epoch of this follower, or -1 if not available.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $replicaId,
        public int $replicaEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 12;
        $writer->writeInt32($this->replicaId);
        $writer->writeInt64($this->replicaEpoch);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $replicaId = $reader->readInt32();
        $replicaEpoch = $reader->readInt64();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($replicaId, $replicaEpoch, $unknownTags);
    }
}

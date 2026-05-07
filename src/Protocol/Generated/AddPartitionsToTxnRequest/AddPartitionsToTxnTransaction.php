<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AddPartitionsToTxnRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AddPartitionsToTxnTransaction implements Encodable, Decodable
{
    /**
     * @param string $transactionalId The transactional id corresponding to the transaction.
     * @param int $producerId Current producer id in use by the transactional id.
     * @param int $producerEpoch Current epoch associated with the producer id.
     * @param bool $verifyOnly Boolean to signify if we want to check if the partition is in the transaction rather than add it.
     * @param list<AddPartitionsToTxnTopic> $topics The partitions to add to the transaction.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public int $producerId,
        public int $producerEpoch,
        public bool $verifyOnly,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeCompactString($this->transactionalId);
        $writer->writeInt64($this->producerId);
        $writer->writeInt16($this->producerEpoch);
        $writer->writeBool($this->verifyOnly);
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $transactionalId = $reader->readCompactString();
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt16();
        $verifyOnly = $reader->readBool();
        $topicsCount = $reader->readCompactArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = AddPartitionsToTxnTopic::decode($reader, $version);
            $topics[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($transactionalId, $producerId, $producerEpoch, $verifyOnly, $topics, $unknownTags);
    }
}

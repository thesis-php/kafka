<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AddPartitionsToTxnResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AddPartitionsToTxnResult implements Encodable, Decodable
{
    /**
     * @param string $transactionalId The transactional id corresponding to the transaction.
     * @param list<AddPartitionsToTxnTopicResult> $topicResults The results for each topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $transactionalId,
        public array $topicResults,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeCompactString($this->transactionalId);
        $writer->writeCompactArrayLength(\count($this->topicResults));
        foreach ($this->topicResults as $item) {
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
        $topicResultsCount = $reader->readCompactArrayLength();
        $topicResults = [];
        for ($topicResultsIndex = 0; $topicResultsIndex < $topicResultsCount; ++$topicResultsIndex) {
            $item = AddPartitionsToTxnTopicResult::decode($reader, $version);
            $topicResults[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($transactionalId, $topicResults, $unknownTags);
    }
}

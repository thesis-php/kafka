<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AddPartitionsToTxnResponse implements Response
{
    /**
     * @param int $throttleTimeMs Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The response top level error code.
     * @param list<AddPartitionsToTxnResponse\AddPartitionsToTxnResult> $resultsByTransaction Results categorized by transactional ID.
     * @param list<AddPartitionsToTxnResponse\AddPartitionsToTxnTopicResult> $resultsByTopicV3AndBelow The results for each topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public array $resultsByTransaction,
        public array $resultsByTopicV3AndBelow,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = 0;
        if ($version >= 4) {
            $errorCode = $reader->readInt16();
        }
        $resultsByTransaction = [];
        if ($version >= 4) {
            $resultsByTransactionCount = $reader->readCompactArrayLength();
            $resultsByTransaction = [];
            for ($resultsByTransactionIndex = 0; $resultsByTransactionIndex < $resultsByTransactionCount; ++$resultsByTransactionIndex) {
                $item = AddPartitionsToTxnResponse\AddPartitionsToTxnResult::decode($reader, $version);
                $resultsByTransaction[] = $item;
            }
        }
        $resultsByTopicV3AndBelow = [];
        if ($version <= 3) {
            if ($flexible) {
                $resultsByTopicV3AndBelowCount = $reader->readCompactArrayLength();
            } else {
                $resultsByTopicV3AndBelowCount = $reader->readArrayLength();
            }
            $resultsByTopicV3AndBelow = [];
            for ($resultsByTopicV3AndBelowIndex = 0; $resultsByTopicV3AndBelowIndex < $resultsByTopicV3AndBelowCount; ++$resultsByTopicV3AndBelowIndex) {
                $item = AddPartitionsToTxnResponse\AddPartitionsToTxnTopicResult::decode($reader, $version);
                $resultsByTopicV3AndBelow[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $resultsByTransaction, $resultsByTopicV3AndBelow, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class InitProducerIdResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $producerId The current producer id.
     * @param int $producerEpoch The current epoch associated with the producer id.
     * @param int $ongoingTxnProducerId The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
     * @param int $ongoingTxnProducerEpoch The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $producerId,
        public int $producerEpoch,
        public int $ongoingTxnProducerId,
        public int $ongoingTxnProducerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $producerId = $reader->readInt64();
        $producerEpoch = $reader->readInt16();
        $ongoingTxnProducerId = -1;
        if ($version >= 6) {
            $ongoingTxnProducerId = $reader->readInt64();
        }
        $ongoingTxnProducerEpoch = -1;
        if ($version >= 6) {
            $ongoingTxnProducerEpoch = $reader->readInt16();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $producerId, $producerEpoch, $ongoingTxnProducerId, $ongoingTxnProducerEpoch, $unknownTags);
    }
}

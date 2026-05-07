<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class EndTxnResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $producerId The producer ID.
     * @param int $producerEpoch The current epoch associated with the producer.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $producerId,
        public int $producerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $producerId = -1;
        if ($version >= 5) {
            $producerId = $reader->readInt64();
        }
        $producerEpoch = -1;
        if ($version >= 5) {
            $producerEpoch = $reader->readInt16();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $producerId, $producerEpoch, $unknownTags);
    }
}

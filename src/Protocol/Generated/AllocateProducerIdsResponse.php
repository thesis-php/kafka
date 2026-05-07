<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AllocateProducerIdsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top level response error code.
     * @param int $producerIdStart The first producer ID in this range, inclusive.
     * @param int $producerIdLen The number of producer IDs in this range.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $producerIdStart,
        public int $producerIdLen,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $producerIdStart = $reader->readInt64();
        $producerIdLen = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $producerIdStart, $producerIdLen, $unknownTags);
    }
}

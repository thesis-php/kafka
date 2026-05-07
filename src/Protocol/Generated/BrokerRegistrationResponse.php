<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class BrokerRegistrationResponse implements Response
{
    /**
     * @param int $throttleTimeMs Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $brokerEpoch The broker's assigned epoch, or -1 if none was assigned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public int $brokerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $brokerEpoch = $reader->readInt64();
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $brokerEpoch, $unknownTags);
    }
}

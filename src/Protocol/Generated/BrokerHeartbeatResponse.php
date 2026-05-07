<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class BrokerHeartbeatResponse implements Response
{
    /**
     * @param int $throttleTimeMs Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param bool $isCaughtUp True if the broker has approximately caught up with the latest metadata.
     * @param bool $isFenced True if the broker is fenced.
     * @param bool $shouldShutDown True if the broker should proceed with its shutdown.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public bool $isCaughtUp,
        public bool $isFenced,
        public bool $shouldShutDown,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $isCaughtUp = $reader->readBool();
        $isFenced = $reader->readBool();
        $shouldShutDown = $reader->readBool();
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $isCaughtUp, $isFenced, $shouldShutDown, $unknownTags);
    }
}

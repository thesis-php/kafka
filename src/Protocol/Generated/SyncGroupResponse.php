<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class SyncGroupResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $protocolType The group protocol type.
     * @param ?string $protocolName The group protocol name.
     * @param string $assignment The member assignment.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $protocolType,
        public ?string $protocolName,
        public string $assignment,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $throttleTimeMs = 0;
        if ($version >= 1) {
            $throttleTimeMs = $reader->readInt32();
        }
        $errorCode = $reader->readInt16();
        $protocolType = null;
        if ($version >= 5) {
            $protocolType = $reader->readCompactNullableString();
        }
        $protocolName = null;
        if ($version >= 5) {
            $protocolName = $reader->readCompactNullableString();
        }
        if ($flexible) {
            $assignment = $reader->readCompactBytes();
        } else {
            $assignment = $reader->readBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $protocolType, $protocolName, $assignment, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class FindCoordinatorResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param int $nodeId The node id.
     * @param string $host The host name.
     * @param int $port The port.
     * @param list<FindCoordinatorResponse\Coordinator> $coordinators Each coordinator result in the response.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public int $nodeId,
        public string $host,
        public int $port,
        public array $coordinators,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $throttleTimeMs = 0;
        if ($version >= 1) {
            $throttleTimeMs = $reader->readInt32();
        }
        $errorCode = 0;
        if ($version <= 3) {
            $errorCode = $reader->readInt16();
        }
        $errorMessage = null;
        if ($version >= 1 && $version <= 3) {
            if ($flexible) {
                $errorMessage = $reader->readCompactNullableString();
            } else {
                $errorMessage = $reader->readNullableString();
            }
        }
        $nodeId = 0;
        if ($version <= 3) {
            $nodeId = $reader->readInt32();
        }
        $host = '';
        if ($version <= 3) {
            if ($flexible) {
                $host = $reader->readCompactString();
            } else {
                $host = $reader->readString();
            }
        }
        $port = 0;
        if ($version <= 3) {
            $port = $reader->readInt32();
        }
        $coordinators = [];
        if ($version >= 4) {
            $coordinatorsCount = $reader->readCompactArrayLength();
            $coordinators = [];
            for ($coordinatorsIndex = 0; $coordinatorsIndex < $coordinatorsCount; ++$coordinatorsIndex) {
                $item = FindCoordinatorResponse\Coordinator::decode($reader, $version);
                $coordinators[] = $item;
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($throttleTimeMs, $errorCode, $errorMessage, $nodeId, $host, $port, $coordinators, $unknownTags);
    }
}

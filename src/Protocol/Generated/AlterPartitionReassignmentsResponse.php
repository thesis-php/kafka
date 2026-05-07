<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AlterPartitionReassignmentsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param bool $allowReplicationFactorChange The option indicating whether changing the replication factor of any given partition as part of the request was allowed.
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The top-level error message, or null if there was no error.
     * @param list<AlterPartitionReassignmentsResponse\ReassignableTopicResponse> $responses The responses to topics to reassign.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public bool $allowReplicationFactorChange,
        public int $errorCode,
        public ?string $errorMessage,
        public array $responses,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $allowReplicationFactorChange = true;
        if ($version >= 1) {
            $allowReplicationFactorChange = $reader->readBool();
        }
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $responsesCount = $reader->readCompactArrayLength();
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = AlterPartitionReassignmentsResponse\ReassignableTopicResponse::decode($reader, $version);
            $responses[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $allowReplicationFactorChange, $errorCode, $errorMessage, $responses, $unknownTags);
    }
}

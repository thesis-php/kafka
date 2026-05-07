<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class AlterShareGroupOffsetsResponse implements Response
{
    /**
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The top-level error message, or null if there was no error.
     * @param list<AlterShareGroupOffsetsResponse\AlterShareGroupOffsetsResponseTopic> $responses The results for each topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $throttleTimeMs,
        public int $errorCode,
        public ?string $errorMessage,
        public array $responses,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $throttleTimeMs = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $responsesCount = $reader->readCompactArrayLength();
        $responses = [];
        for ($responsesIndex = 0; $responsesIndex < $responsesCount; ++$responsesIndex) {
            $item = AlterShareGroupOffsetsResponse\AlterShareGroupOffsetsResponseTopic::decode($reader, $version);
            $responses[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($throttleTimeMs, $errorCode, $errorMessage, $responses, $unknownTags);
    }
}

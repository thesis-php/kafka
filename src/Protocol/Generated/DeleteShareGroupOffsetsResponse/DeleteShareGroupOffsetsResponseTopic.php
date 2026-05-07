<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteShareGroupOffsetsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteShareGroupOffsetsResponseTopic implements Encodable, Decodable
{
    /**
     * @param string $topicName The topic name.
     * @param string $topicId The unique topic ID.
     * @param int $errorCode The topic-level error code, or 0 if there was no error.
     * @param ?string $errorMessage The topic-level error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $topicName,
        public string $topicId,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->topicName);
        $writer->writeUuid($this->topicId);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $topicName = $reader->readCompactString();
        $topicId = $reader->readUuid();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($topicName, $topicId, $errorCode, $errorMessage, $unknownTags);
    }
}

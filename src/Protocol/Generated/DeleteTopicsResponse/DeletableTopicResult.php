<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteTopicsResponse;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeletableTopicResult implements Encodable, Decodable
{
    /**
     * @param ?string $name The topic name.
     * @param UuidInterface $topicId The unique topic ID.
     * @param int $errorCode The deletion error, or 0 if the deletion succeeded.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $name,
        public UuidInterface $topicId,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($version >= 6) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->name);
            } else {
                $writer->writeNullableString($this->name);
            }
        } else {
            if ($this->name === null) {
                throw new \UnexpectedValueException("Field 'Name' must not be null in version '{$version}'");
            }
            if ($flexible) {
                $writer->writeCompactString($this->name);
            } else {
                $writer->writeString($this->name);
            }
        }
        if ($version >= 6) {
            $writer->writeUuid($this->topicId);
        }
        $writer->writeInt16($this->errorCode);
        if ($version >= 5) {
            $writer->writeCompactNullableString($this->errorMessage);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        if ($version >= 6) {
            if ($flexible) {
                $name = $reader->readCompactNullableString();
            } else {
                $name = $reader->readNullableString();
            }
        } elseif ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 6) {
            $topicId = $reader->readUuid();
        }
        $errorCode = $reader->readInt16();
        $errorMessage = null;
        if ($version >= 5) {
            $errorMessage = $reader->readCompactNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $errorCode, $errorMessage, $unknownTags);
    }
}

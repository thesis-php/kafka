<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\MetadataRequest;

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
final readonly class MetadataRequestTopic implements Encodable, Decodable
{
    /**
     * @param UuidInterface $topicId The topic id.
     * @param ?string $name The topic name.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public UuidInterface $topicId,
        public ?string $name,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        if ($version >= 10) {
            $writer->writeUuid($this->topicId);
        }
        if ($version >= 10) {
            $writer->writeCompactNullableString($this->name);
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
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $topicId = Uuid::fromString(Uuid::NIL);
        if ($version >= 10) {
            $topicId = $reader->readUuid();
        }
        if ($version >= 10) {
            $name = $reader->readCompactNullableString();
        } elseif ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($topicId, $name, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteTopicsRequest;

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
final readonly class DeleteTopicState implements Encodable, Decodable
{
    /**
     * @param ?string $name The topic name.
     * @param UuidInterface $topicId The unique topic ID.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $name,
        public UuidInterface $topicId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        $writer->writeCompactNullableString($this->name);
        $writer->writeUuid($this->topicId);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 4;
        $name = $reader->readCompactNullableString();
        $topicId = $reader->readUuid();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $topicId, $unknownTags);
    }
}

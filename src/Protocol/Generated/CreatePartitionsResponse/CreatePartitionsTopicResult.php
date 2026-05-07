<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreatePartitionsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatePartitionsTopicResult implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param int $errorCode The result error, or zero if there was no error.
     * @param ?string $errorMessage The result message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeCompactNullableString($this->errorMessage);
        } else {
            $writer->writeNullableString($this->errorMessage);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $errorMessage = $reader->readCompactNullableString();
        } else {
            $errorMessage = $reader->readNullableString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $errorCode, $errorMessage, $unknownTags);
    }
}

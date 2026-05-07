<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\JoinGroupRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class JoinGroupRequestProtocol implements Encodable, Decodable
{
    /**
     * @param string $name The protocol name.
     * @param string $metadata The protocol metadata.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $metadata,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        if ($flexible) {
            $writer->writeCompactBytes($this->metadata);
        } else {
            $writer->writeBytes($this->metadata);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 6;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        if ($flexible) {
            $metadata = $reader->readCompactBytes();
        } else {
            $metadata = $reader->readBytes();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($name, $metadata, $unknownTags);
    }
}

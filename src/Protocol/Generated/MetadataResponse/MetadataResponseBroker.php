<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\MetadataResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class MetadataResponseBroker implements Encodable, Decodable
{
    /**
     * @param int $nodeId The broker ID.
     * @param string $host The broker hostname.
     * @param int $port The broker port.
     * @param ?string $rack The rack of the broker, or null if it has not been assigned to a rack.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $nodeId,
        public string $host,
        public int $port,
        public ?string $rack,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        $writer->writeInt32($this->nodeId);
        if ($flexible) {
            $writer->writeCompactString($this->host);
        } else {
            $writer->writeString($this->host);
        }
        $writer->writeInt32($this->port);
        if ($version >= 1) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->rack);
            } else {
                $writer->writeNullableString($this->rack);
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
        $nodeId = $reader->readInt32();
        if ($flexible) {
            $host = $reader->readCompactString();
        } else {
            $host = $reader->readString();
        }
        $port = $reader->readInt32();
        $rack = null;
        if ($version >= 1) {
            if ($flexible) {
                $rack = $reader->readCompactNullableString();
            } else {
                $rack = $reader->readNullableString();
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($nodeId, $host, $port, $rack, $unknownTags);
    }
}

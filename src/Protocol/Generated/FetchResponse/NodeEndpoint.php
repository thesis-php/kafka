<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FetchResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class NodeEndpoint implements Encodable, Decodable
{
    /**
     * @param int $nodeId The ID of the associated node.
     * @param string $host The node's hostname.
     * @param int $port The node's port.
     * @param ?string $rack The rack of the node, or null if it has not been assigned to a rack.
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
        $flexible = $version >= 12;
        $writer->writeInt32($this->nodeId);
        $writer->writeCompactString($this->host);
        $writer->writeInt32($this->port);
        $writer->writeCompactNullableString($this->rack);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 12;
        $nodeId = $reader->readInt32();
        $host = $reader->readCompactString();
        $port = $reader->readInt32();
        $rack = $reader->readCompactNullableString();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($nodeId, $host, $port, $rack, $unknownTags);
    }
}

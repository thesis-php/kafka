<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\VotersRecord\Voter;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Endpoint implements Encodable, Decodable
{
    /**
     * @param string $name The name of the endpoint.
     * @param string $host The hostname.
     * @param int $port The port.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $host,
        public int $port,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeCompactString($this->host);
        $writer->writeUint16($this->port);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $host = $reader->readCompactString();
        $port = $reader->readUint16();
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $host, $port, $unknownTags);
    }
}

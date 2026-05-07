<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeClusterResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeClusterBroker implements Encodable, Decodable
{
    /**
     * @param int $brokerId The broker ID.
     * @param string $host The broker hostname.
     * @param int $port The broker port.
     * @param ?string $rack The rack of the broker, or null if it has not been assigned to a rack.
     * @param bool $isFenced Whether the broker is fenced
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public string $host,
        public int $port,
        public ?string $rack,
        public bool $isFenced,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeCompactString($this->host);
        $writer->writeInt32($this->port);
        $writer->writeCompactNullableString($this->rack);
        if ($version >= 2) {
            $writer->writeBool($this->isFenced);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $brokerId = $reader->readInt32();
        $host = $reader->readCompactString();
        $port = $reader->readInt32();
        $rack = $reader->readCompactNullableString();
        $isFenced = false;
        if ($version >= 2) {
            $isFenced = $reader->readBool();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($brokerId, $host, $port, $rack, $isFenced, $unknownTags);
    }
}

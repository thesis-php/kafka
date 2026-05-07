<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\FindCoordinatorResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Coordinator implements Encodable, Decodable
{
    /**
     * @param string $key The coordinator key.
     * @param int $nodeId The node id.
     * @param string $host The host name.
     * @param int $port The port.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $key,
        public int $nodeId,
        public string $host,
        public int $port,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        $writer->writeCompactString($this->key);
        $writer->writeInt32($this->nodeId);
        $writer->writeCompactString($this->host);
        $writer->writeInt32($this->port);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 3;
        $key = $reader->readCompactString();
        $nodeId = $reader->readInt32();
        $host = $reader->readCompactString();
        $port = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($key, $nodeId, $host, $port, $errorCode, $errorMessage, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class RequestHeader implements Encodable, Decodable
{
    /**
     * @param int $requestApiKey The API key of this request.
     * @param int $requestApiVersion The API version of this request.
     * @param int $correlationId The correlation ID of this request.
     * @param ?string $clientId The client ID string.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $requestApiKey,
        public int $requestApiVersion,
        public int $correlationId,
        public ?string $clientId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt16($this->requestApiKey);
        $writer->writeInt16($this->requestApiVersion);
        $writer->writeInt32($this->correlationId);
        $writer->writeNullableString($this->clientId);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $requestApiKey = $reader->readInt16();
        $requestApiVersion = $reader->readInt16();
        $correlationId = $reader->readInt32();
        $clientId = $reader->readNullableString();
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($requestApiKey, $requestApiVersion, $correlationId, $clientId, $unknownTags);
    }
}

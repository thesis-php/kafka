<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\InitializeShareGroupStateResponse\InitializeStateResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionResult implements Encodable, Decodable
{
    /**
     * @param int $partition The partition index.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $partition,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->partition);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $partition = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($partition, $errorCode, $errorMessage, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class EnvelopeResponse implements Response
{
    /**
     * @param ?string $responseData The embedded response header and data.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $responseData,
        public int $errorCode,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $responseData = $reader->readCompactNullableBytes();
        $errorCode = $reader->readInt16();
        $unknownTags = $reader->readTaggedFields();
        return new self($responseData, $errorCode, $unknownTags);
    }
}

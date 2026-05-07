<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\UpdateFeaturesResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class UpdatableFeatureResult implements Encodable, Decodable
{
    /**
     * @param string $feature The name of the finalized feature.
     * @param int $errorCode The feature update error code or `0` if the feature update succeeded.
     * @param ?string $errorMessage The feature update error, or `null` if the feature update succeeded.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $feature,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->feature);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $feature = $reader->readCompactString();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($feature, $errorCode, $errorMessage, $unknownTags);
    }
}

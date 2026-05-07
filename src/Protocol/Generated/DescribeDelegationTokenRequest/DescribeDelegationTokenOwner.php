<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeDelegationTokenRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeDelegationTokenOwner implements Encodable, Decodable
{
    /**
     * @param string $principalType The owner principal type.
     * @param string $principalName The owner principal name.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $principalType,
        public string $principalName,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactString($this->principalType);
        } else {
            $writer->writeString($this->principalType);
        }
        if ($flexible) {
            $writer->writeCompactString($this->principalName);
        } else {
            $writer->writeString($this->principalName);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $principalType = $reader->readCompactString();
        } else {
            $principalType = $reader->readString();
        }
        if ($flexible) {
            $principalName = $reader->readCompactString();
        } else {
            $principalName = $reader->readString();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($principalType, $principalName, $unknownTags);
    }
}

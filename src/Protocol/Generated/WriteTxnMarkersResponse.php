<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class WriteTxnMarkersResponse implements Response
{
    /**
     * @param list<WriteTxnMarkersResponse\WritableTxnMarkerResult> $markers The results for writing makers.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $markers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $markersCount = $reader->readCompactArrayLength();
        } else {
            $markersCount = $reader->readArrayLength();
        }
        $markers = [];
        for ($markersIndex = 0; $markersIndex < $markersCount; ++$markersIndex) {
            $item = WriteTxnMarkersResponse\WritableTxnMarkerResult::decode($reader, $version);
            $markers[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($markers, $unknownTags);
    }
}

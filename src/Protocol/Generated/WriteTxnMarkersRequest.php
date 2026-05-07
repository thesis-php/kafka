<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<WriteTxnMarkersResponse>
 */
final readonly class WriteTxnMarkersRequest implements Request
{
    /**
     * @param list<WriteTxnMarkersRequest\WritableTxnMarker> $markers The transaction markers to be written.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $markers,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::WriteTxnMarkers;
    }

    #[\Override]
    public function responseType(): string
    {
        return WriteTxnMarkersResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->markers));
        } else {
            $writer->writeArrayLength(\count($this->markers));
        }
        foreach ($this->markers as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

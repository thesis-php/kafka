<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterClientQuotasResponse>
 */
final readonly class AlterClientQuotasRequest implements Request
{
    /**
     * @param list<AlterClientQuotasRequest\EntryData> $entries The quota configuration entries to alter.
     * @param bool $validateOnly Whether the alteration should be validated, but not performed.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $entries,
        public bool $validateOnly,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterClientQuotas;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterClientQuotasResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->entries));
        } else {
            $writer->writeArrayLength(\count($this->entries));
        }
        foreach ($this->entries as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeBool($this->validateOnly);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

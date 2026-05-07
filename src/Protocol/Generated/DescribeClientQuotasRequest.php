<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeClientQuotasResponse>
 */
final readonly class DescribeClientQuotasRequest implements Request
{
    /**
     * @param list<DescribeClientQuotasRequest\ComponentData> $components Filter components to apply to quota entities.
     * @param bool $strict Whether the match is strict, i.e. should exclude entities with unspecified entity types.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $components,
        public bool $strict,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeClientQuotas;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeClientQuotasResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 1;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->components));
        } else {
            $writer->writeArrayLength(\count($this->components));
        }
        foreach ($this->components as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeBool($this->strict);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

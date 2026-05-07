<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeConfigsResponse>
 */
final readonly class DescribeConfigsRequest implements Request
{
    /**
     * @param list<DescribeConfigsRequest\DescribeConfigsResource> $resources The resources whose configurations we want to describe.
     * @param bool $includeSynonyms True if we should include all synonyms.
     * @param bool $includeDocumentation True if we should include configuration documentation.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $resources,
        public bool $includeSynonyms,
        public bool $includeDocumentation,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeConfigs;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeConfigsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->resources));
        } else {
            $writer->writeArrayLength(\count($this->resources));
        }
        foreach ($this->resources as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeBool($this->includeSynonyms);
        if ($version >= 3) {
            $writer->writeBool($this->includeDocumentation);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

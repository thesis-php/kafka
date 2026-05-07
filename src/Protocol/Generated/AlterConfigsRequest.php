<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterConfigsResponse>
 */
final readonly class AlterConfigsRequest implements Request
{
    /**
     * @param list<AlterConfigsRequest\AlterConfigsResource> $resources The updates for each resource.
     * @param bool $validateOnly True if we should validate the request, but not change the configurations.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $resources,
        public bool $validateOnly,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterConfigs;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterConfigsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->resources));
        } else {
            $writer->writeArrayLength(\count($this->resources));
        }
        foreach ($this->resources as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeBool($this->validateOnly);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

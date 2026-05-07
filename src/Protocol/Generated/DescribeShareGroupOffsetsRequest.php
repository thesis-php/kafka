<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeShareGroupOffsetsResponse>
 */
final readonly class DescribeShareGroupOffsetsRequest implements Request
{
    /**
     * @param list<DescribeShareGroupOffsetsRequest\DescribeShareGroupOffsetsRequestGroup> $groups The groups to describe offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $groups,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeShareGroupOffsets;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeShareGroupOffsetsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->groups));
        foreach ($this->groups as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

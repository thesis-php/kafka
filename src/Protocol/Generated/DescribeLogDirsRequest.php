<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeLogDirsResponse>
 */
final readonly class DescribeLogDirsRequest implements Request
{
    /**
     * @param ?list<DescribeLogDirsRequest\DescribableLogDirTopic> $topics Each topic that we want to describe log directories for, or null for all topics.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeLogDirs;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeLogDirsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactNullableArrayLength($this->topics === null ? null : \count($this->topics));
        } else {
            $writer->writeNullableArrayLength($this->topics === null ? null : \count($this->topics));
        }
        if ($this->topics !== null) {
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

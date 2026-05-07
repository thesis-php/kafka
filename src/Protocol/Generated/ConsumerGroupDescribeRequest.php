<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ConsumerGroupDescribeResponse>
 */
final readonly class ConsumerGroupDescribeRequest implements Request
{
    /**
     * @param list<string> $groupIds The ids of the groups to describe.
     * @param bool $includeAuthorizedOperations Whether to include authorized operations.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $groupIds,
        public bool $includeAuthorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ConsumerGroupDescribe;
    }

    #[\Override]
    public function responseType(): string
    {
        return ConsumerGroupDescribeResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->groupIds));
        foreach ($this->groupIds as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeBool($this->includeAuthorizedOperations);
        $writer->writeTaggedFields($this->unknownTags);
    }
}

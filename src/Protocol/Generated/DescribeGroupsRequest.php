<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeGroupsResponse>
 */
final readonly class DescribeGroupsRequest implements Request
{
    /**
     * @param list<string> $groups The names of the groups to describe.
     * @param bool $includeAuthorizedOperations Whether to include authorized operations.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $groups,
        public bool $includeAuthorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeGroups;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeGroupsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->groups));
        } else {
            $writer->writeArrayLength(\count($this->groups));
        }
        foreach ($this->groups as $item) {
            if ($flexible) {
                $writer->writeCompactString($item);
            } else {
                $writer->writeString($item);
            }
        }
        if ($version >= 3) {
            $writer->writeBool($this->includeAuthorizedOperations);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

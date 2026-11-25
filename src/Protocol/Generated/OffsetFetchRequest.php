<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<OffsetFetchResponse>
 */
final readonly class OffsetFetchRequest implements Request
{
    /**
     * @param string $groupId The group to fetch offsets for.
     * @param ?list<OffsetFetchRequest\OffsetFetchRequestTopic> $topics Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
     * @param list<OffsetFetchRequest\OffsetFetchRequestGroup> $groups Each group we would like to fetch offsets for.
     * @param bool $requireStable Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public ?array $topics,
        public array $groups,
        public bool $requireStable,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::OffsetFetch;
    }

    #[\Override]
    public function responseType(): string
    {
        return OffsetFetchResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($version <= 7) {
            if ($flexible) {
                $writer->writeCompactString($this->groupId);
            } else {
                $writer->writeString($this->groupId);
            }
        }
        if ($version <= 7) {
            if ($version >= 2) {
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
            } else {
                if ($this->topics === null) {
                    throw new \UnexpectedValueException("Field 'Topics' must not be null in version '{$version}'");
                }
                $writer->writeArrayLength(\count($this->topics));
                foreach ($this->topics as $item) {
                    $item->encode($writer, $version);
                }
            }
        }
        if ($version >= 8) {
            $writer->writeCompactArrayLength(\count($this->groups));
            foreach ($this->groups as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 7) {
            $writer->writeBool($this->requireStable);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

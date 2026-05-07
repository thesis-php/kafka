<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<OffsetCommitResponse>
 */
final readonly class OffsetCommitRequest implements Request
{
    /**
     * @param string $groupId The unique group identifier.
     * @param int $generationIdOrMemberEpoch The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
     * @param string $memberId The member ID assigned by the group coordinator.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param int $retentionTimeMs The time period in ms to retain the offset.
     * @param list<OffsetCommitRequest\OffsetCommitRequestTopic> $topics The topics to commit offsets for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public int $generationIdOrMemberEpoch,
        public string $memberId,
        public ?string $groupInstanceId,
        public int $retentionTimeMs,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::OffsetCommit;
    }

    #[\Override]
    public function responseType(): string
    {
        return OffsetCommitResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 8;
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        $writer->writeInt32($this->generationIdOrMemberEpoch);
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($version >= 7) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->groupInstanceId);
            } else {
                $writer->writeNullableString($this->groupInstanceId);
            }
        }
        if ($version <= 4) {
            $writer->writeInt64($this->retentionTimeMs);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

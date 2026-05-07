<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<SyncGroupResponse>
 */
final readonly class SyncGroupRequest implements Request
{
    /**
     * @param string $groupId The unique group identifier.
     * @param int $generationId The generation of the group.
     * @param string $memberId The member ID assigned by the group.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param ?string $protocolType The group protocol type.
     * @param ?string $protocolName The group protocol name.
     * @param list<SyncGroupRequest\SyncGroupRequestAssignment> $assignments Each assignment.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public int $generationId,
        public string $memberId,
        public ?string $groupInstanceId,
        public ?string $protocolType,
        public ?string $protocolName,
        public array $assignments,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::SyncGroup;
    }

    #[\Override]
    public function responseType(): string
    {
        return SyncGroupResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        $writer->writeInt32($this->generationId);
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($version >= 3) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->groupInstanceId);
            } else {
                $writer->writeNullableString($this->groupInstanceId);
            }
        }
        if ($version >= 5) {
            $writer->writeCompactNullableString($this->protocolType);
        }
        if ($version >= 5) {
            $writer->writeCompactNullableString($this->protocolName);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->assignments));
        } else {
            $writer->writeArrayLength(\count($this->assignments));
        }
        foreach ($this->assignments as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

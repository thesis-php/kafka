<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<LeaveGroupResponse>
 */
final readonly class LeaveGroupRequest implements Request
{
    /**
     * @param string $groupId The ID of the group to leave.
     * @param string $memberId The member ID to remove from the group.
     * @param list<LeaveGroupRequest\MemberIdentity> $members List of leaving member identities.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public string $memberId,
        public array $members,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::LeaveGroup;
    }

    #[\Override]
    public function responseType(): string
    {
        return LeaveGroupResponse::class;
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
        if ($version <= 2) {
            $writer->writeString($this->memberId);
        }
        if ($version >= 3) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->members));
            } else {
                $writer->writeArrayLength(\count($this->members));
            }
            foreach ($this->members as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

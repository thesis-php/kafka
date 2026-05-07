<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<HeartbeatResponse>
 */
final readonly class HeartbeatRequest implements Request
{
    /**
     * @param string $groupId The group id.
     * @param int $generationId The generation of the group.
     * @param string $memberId The member ID.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public int $generationId,
        public string $memberId,
        public ?string $groupInstanceId,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Heartbeat;
    }

    #[\Override]
    public function responseType(): string
    {
        return HeartbeatResponse::class;
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
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

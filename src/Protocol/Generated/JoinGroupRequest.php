<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<JoinGroupResponse>
 */
final readonly class JoinGroupRequest implements Request
{
    /**
     * @param string $groupId The group identifier.
     * @param int $sessionTimeoutMs The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
     * @param int $rebalanceTimeoutMs The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
     * @param string $memberId The member id assigned by the group coordinator.
     * @param ?string $groupInstanceId The unique identifier of the consumer instance provided by end user.
     * @param string $protocolType The unique name the for class of protocols implemented by the group we want to join.
     * @param list<JoinGroupRequest\JoinGroupRequestProtocol> $protocols The list of protocols that the member supports.
     * @param ?string $reason The reason why the member (re-)joins the group.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $groupId,
        public int $sessionTimeoutMs,
        public int $rebalanceTimeoutMs,
        public string $memberId,
        public ?string $groupInstanceId,
        public string $protocolType,
        public array $protocols,
        public ?string $reason,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::JoinGroup;
    }

    #[\Override]
    public function responseType(): string
    {
        return JoinGroupResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 6;
        if ($flexible) {
            $writer->writeCompactString($this->groupId);
        } else {
            $writer->writeString($this->groupId);
        }
        $writer->writeInt32($this->sessionTimeoutMs);
        if ($version >= 1) {
            $writer->writeInt32($this->rebalanceTimeoutMs);
        }
        if ($flexible) {
            $writer->writeCompactString($this->memberId);
        } else {
            $writer->writeString($this->memberId);
        }
        if ($version >= 5) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->groupInstanceId);
            } else {
                $writer->writeNullableString($this->groupInstanceId);
            }
        }
        if ($flexible) {
            $writer->writeCompactString($this->protocolType);
        } else {
            $writer->writeString($this->protocolType);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->protocols));
        } else {
            $writer->writeArrayLength(\count($this->protocols));
        }
        foreach ($this->protocols as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 8) {
            $writer->writeCompactNullableString($this->reason);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

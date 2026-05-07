<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ShareAcknowledgeResponse>
 */
final readonly class ShareAcknowledgeRequest implements Request
{
    /**
     * @param ?string $groupId The group identifier.
     * @param ?string $memberId The member ID.
     * @param int $shareSessionEpoch The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
     * @param bool $isRenewAck Whether Renew type acknowledgements present in AcknowledgementBatches.
     * @param list<ShareAcknowledgeRequest\AcknowledgeTopic> $topics The topics containing records to acknowledge.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?string $groupId,
        public ?string $memberId,
        public int $shareSessionEpoch,
        public bool $isRenewAck,
        public array $topics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ShareAcknowledge;
    }

    #[\Override]
    public function responseType(): string
    {
        return ShareAcknowledgeResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableString($this->groupId);
        $writer->writeCompactNullableString($this->memberId);
        $writer->writeInt32($this->shareSessionEpoch);
        if ($version >= 2) {
            $writer->writeBool($this->isRenewAck);
        }
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

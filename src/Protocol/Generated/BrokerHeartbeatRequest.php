<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<BrokerHeartbeatResponse>
 */
final readonly class BrokerHeartbeatRequest implements Request
{
    /**
     * @param int $brokerId The broker ID.
     * @param int $brokerEpoch The broker epoch.
     * @param int $currentMetadataOffset The highest metadata offset which the broker has reached.
     * @param bool $wantFence True if the broker wants to be fenced, false otherwise.
     * @param bool $wantShutDown True if the broker wants to be shut down, false otherwise.
     * @param list<UuidInterface> $offlineLogDirs Log directories that failed and went offline.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public int $brokerEpoch,
        public int $currentMetadataOffset,
        public bool $wantFence,
        public bool $wantShutDown,
        public array $offlineLogDirs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::BrokerHeartbeat;
    }

    #[\Override]
    public function responseType(): string
    {
        return BrokerHeartbeatResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeInt64($this->brokerEpoch);
        $writer->writeInt64($this->currentMetadataOffset);
        $writer->writeBool($this->wantFence);
        $writer->writeBool($this->wantShutDown);
        $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
        if ($version >= 1) {
            if (isset($taggedFields[0])) {
                throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'OfflineLogDirs\'.');
            }
            $tagWriter = new Writer();
            $tagWriter->writeCompactArrayLength(\count($this->offlineLogDirs));
            foreach ($this->offlineLogDirs as $item) {
                $tagWriter->writeUuid($item);
            }
            $taggedFields[0] = (string) $tagWriter;
        }
        $writer->writeTaggedFields($taggedFields);
    }
}

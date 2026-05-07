<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<BrokerRegistrationResponse>
 */
final readonly class BrokerRegistrationRequest implements Request
{
    /**
     * @param int $brokerId The broker ID.
     * @param string $clusterId The cluster id of the broker process.
     * @param UuidInterface $incarnationId The incarnation id of the broker process.
     * @param list<BrokerRegistrationRequest\Listener> $listeners The listeners of this broker.
     * @param list<BrokerRegistrationRequest\Feature> $features The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
     * @param ?string $rack The rack which this broker is in.
     * @param bool $isMigratingZkBroker If the required configurations for ZK migration are present, this value is set to true.
     * @param list<UuidInterface> $logDirs Log directories configured in this broker which are available.
     * @param int $previousBrokerEpoch The epoch before a clean shutdown.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $brokerId,
        public string $clusterId,
        public UuidInterface $incarnationId,
        public array $listeners,
        public array $features,
        public ?string $rack,
        public bool $isMigratingZkBroker,
        public array $logDirs,
        public int $previousBrokerEpoch,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::BrokerRegistration;
    }

    #[\Override]
    public function responseType(): string
    {
        return BrokerRegistrationResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->brokerId);
        $writer->writeCompactString($this->clusterId);
        $writer->writeUuid($this->incarnationId);
        $writer->writeCompactArrayLength(\count($this->listeners));
        foreach ($this->listeners as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->features));
        foreach ($this->features as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactNullableString($this->rack);
        if ($version >= 1) {
            $writer->writeBool($this->isMigratingZkBroker);
        }
        if ($version >= 2) {
            $writer->writeCompactArrayLength(\count($this->logDirs));
            foreach ($this->logDirs as $item) {
                $writer->writeUuid($item);
            }
        }
        if ($version >= 3) {
            $writer->writeInt64($this->previousBrokerEpoch);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<PushTelemetryResponse>
 */
final readonly class PushTelemetryRequest implements Request
{
    /**
     * @param UuidInterface $clientInstanceId Unique id for this client instance.
     * @param int $subscriptionId Unique identifier for the current subscription.
     * @param bool $terminating Client is terminating the connection.
     * @param int $compressionType Compression codec used to compress the metrics.
     * @param string $metrics Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public UuidInterface $clientInstanceId,
        public int $subscriptionId,
        public bool $terminating,
        public int $compressionType,
        public string $metrics,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::PushTelemetry;
    }

    #[\Override]
    public function responseType(): string
    {
        return PushTelemetryResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeUuid($this->clientInstanceId);
        $writer->writeInt32($this->subscriptionId);
        $writer->writeBool($this->terminating);
        $writer->writeInt8($this->compressionType);
        $writer->writeCompactBytes($this->metrics);
        $writer->writeTaggedFields($this->unknownTags);
    }
}

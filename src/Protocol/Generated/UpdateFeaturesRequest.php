<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<UpdateFeaturesResponse>
 */
final readonly class UpdateFeaturesRequest implements Request
{
    /**
     * @param int $timeoutMs How long to wait in milliseconds before timing out the request.
     * @param list<UpdateFeaturesRequest\FeatureUpdateKey> $featureUpdates The list of updates to finalized features.
     * @param bool $validateOnly True if we should validate the request, but not perform the upgrade or downgrade.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $timeoutMs,
        public array $featureUpdates,
        public bool $validateOnly,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::UpdateFeatures;
    }

    #[\Override]
    public function responseType(): string
    {
        return UpdateFeaturesResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->timeoutMs);
        $writer->writeCompactArrayLength(\count($this->featureUpdates));
        foreach ($this->featureUpdates as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 1) {
            $writer->writeBool($this->validateOnly);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

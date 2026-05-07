<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\UpdateFeaturesRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class FeatureUpdateKey implements Encodable, Decodable
{
    /**
     * @param string $feature The name of the finalized feature to be updated.
     * @param int $maxVersionLevel The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
     * @param bool $allowDowngrade DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
     * @param int $upgradeType Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $feature,
        public int $maxVersionLevel,
        public bool $allowDowngrade,
        public int $upgradeType,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->feature);
        $writer->writeInt16($this->maxVersionLevel);
        if ($version <= 0) {
            $writer->writeBool($this->allowDowngrade);
        }
        if ($version >= 1) {
            $writer->writeInt8($this->upgradeType);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $feature = $reader->readCompactString();
        $maxVersionLevel = $reader->readInt16();
        $allowDowngrade = false;
        if ($version <= 0) {
            $allowDowngrade = $reader->readBool();
        }
        $upgradeType = 1;
        if ($version >= 1) {
            $upgradeType = $reader->readInt8();
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($feature, $maxVersionLevel, $allowDowngrade, $upgradeType, $unknownTags);
    }
}

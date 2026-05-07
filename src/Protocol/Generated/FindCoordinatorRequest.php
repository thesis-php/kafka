<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<FindCoordinatorResponse>
 */
final readonly class FindCoordinatorRequest implements Request
{
    /**
     * @param string $key The coordinator key.
     * @param int $keyType The coordinator key type. (group, transaction, share).
     * @param list<string> $coordinatorKeys The coordinator keys.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $key,
        public int $keyType,
        public array $coordinatorKeys,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::FindCoordinator;
    }

    #[\Override]
    public function responseType(): string
    {
        return FindCoordinatorResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($version <= 3) {
            if ($flexible) {
                $writer->writeCompactString($this->key);
            } else {
                $writer->writeString($this->key);
            }
        }
        if ($version >= 1) {
            $writer->writeInt8($this->keyType);
        }
        if ($version >= 4) {
            $writer->writeCompactArrayLength(\count($this->coordinatorKeys));
            foreach ($this->coordinatorKeys as $item) {
                $writer->writeCompactString($item);
            }
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

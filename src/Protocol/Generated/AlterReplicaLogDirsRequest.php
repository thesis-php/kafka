<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterReplicaLogDirsResponse>
 */
final readonly class AlterReplicaLogDirsRequest implements Request
{
    /**
     * @param list<AlterReplicaLogDirsRequest\AlterReplicaLogDir> $dirs The alterations to make for each directory.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $dirs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterReplicaLogDirs;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterReplicaLogDirsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->dirs));
        } else {
            $writer->writeArrayLength(\count($this->dirs));
        }
        foreach ($this->dirs as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

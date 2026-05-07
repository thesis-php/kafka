<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<AlterUserScramCredentialsResponse>
 */
final readonly class AlterUserScramCredentialsRequest implements Request
{
    /**
     * @param list<AlterUserScramCredentialsRequest\ScramCredentialDeletion> $deletions The SCRAM credentials to remove.
     * @param list<AlterUserScramCredentialsRequest\ScramCredentialUpsertion> $upsertions The SCRAM credentials to update/insert.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $deletions,
        public array $upsertions,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::AlterUserScramCredentials;
    }

    #[\Override]
    public function responseType(): string
    {
        return AlterUserScramCredentialsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->deletions));
        foreach ($this->deletions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->upsertions));
        foreach ($this->upsertions as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

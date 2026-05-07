<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<MetadataResponse>
 */
final readonly class MetadataRequest implements Request
{
    /**
     * @param ?list<MetadataRequest\MetadataRequestTopic> $topics The topics to fetch metadata for.
     * @param bool $allowAutoTopicCreation If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
     * @param bool $includeClusterAuthorizedOperations Whether to include cluster authorized operations.
     * @param bool $includeTopicAuthorizedOperations Whether to include topic authorized operations.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?array $topics,
        public bool $allowAutoTopicCreation,
        public bool $includeClusterAuthorizedOperations,
        public bool $includeTopicAuthorizedOperations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::Metadata;
    }

    #[\Override]
    public function responseType(): string
    {
        return MetadataResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        if ($version >= 1) {
            if ($flexible) {
                $writer->writeCompactNullableArrayLength($this->topics === null ? null : \count($this->topics));
            } else {
                $writer->writeNullableArrayLength($this->topics === null ? null : \count($this->topics));
            }
            if ($this->topics !== null) {
                foreach ($this->topics as $item) {
                    $item->encode($writer, $version);
                }
            }
        } else {
            if ($this->topics === null) {
                throw new \UnexpectedValueException("Field 'Topics' must not be null in version '{$version}'");
            }
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->topics));
            } else {
                $writer->writeArrayLength(\count($this->topics));
            }
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 4) {
            $writer->writeBool($this->allowAutoTopicCreation);
        }
        if ($version >= 8 && $version <= 10) {
            $writer->writeBool($this->includeClusterAuthorizedOperations);
        }
        if ($version >= 8) {
            $writer->writeBool($this->includeTopicAuthorizedOperations);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

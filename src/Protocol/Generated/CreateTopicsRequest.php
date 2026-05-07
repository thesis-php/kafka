<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<CreateTopicsResponse>
 */
final readonly class CreateTopicsRequest implements Request
{
    /**
     * @param list<CreateTopicsRequest\CreatableTopic> $topics The topics to create.
     * @param int $timeoutMs How long to wait in milliseconds before timing out the request.
     * @param bool $validateOnly If true, check that the topics can be created as specified, but don't create anything.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topics,
        public int $timeoutMs,
        public bool $validateOnly,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::CreateTopics;
    }

    #[\Override]
    public function responseType(): string
    {
        return CreateTopicsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->timeoutMs);
        $writer->writeBool($this->validateOnly);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

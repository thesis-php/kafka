<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DeleteTopicsResponse>
 */
final readonly class DeleteTopicsRequest implements Request
{
    /**
     * @param list<DeleteTopicsRequest\DeleteTopicState> $topics The name or topic ID of the topic.
     * @param list<string> $topicNames The names of the topics to delete.
     * @param int $timeoutMs The length of time in milliseconds to wait for the deletions to complete.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topics,
        public array $topicNames,
        public int $timeoutMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DeleteTopics;
    }

    #[\Override]
    public function responseType(): string
    {
        return DeleteTopicsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 4;
        if ($version >= 6) {
            $writer->writeCompactArrayLength(\count($this->topics));
            foreach ($this->topics as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version <= 5) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->topicNames));
            } else {
                $writer->writeArrayLength(\count($this->topicNames));
            }
            foreach ($this->topicNames as $item) {
                if ($flexible) {
                    $writer->writeCompactString($item);
                } else {
                    $writer->writeString($item);
                }
            }
        }
        $writer->writeInt32($this->timeoutMs);
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

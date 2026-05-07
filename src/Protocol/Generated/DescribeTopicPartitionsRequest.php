<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeTopicPartitionsResponse>
 */
final readonly class DescribeTopicPartitionsRequest implements Request
{
    /**
     * @param list<DescribeTopicPartitionsRequest\TopicRequest> $topics The topics to fetch details for.
     * @param int $responsePartitionLimit The maximum number of partitions included in the response.
     * @param ?DescribeTopicPartitionsRequest\Cursor $cursor The first topic and partition index to fetch details for.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $topics,
        public int $responsePartitionLimit,
        public ?DescribeTopicPartitionsRequest\Cursor $cursor,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeTopicPartitions;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeTopicPartitionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->topics));
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeInt32($this->responsePartitionLimit);
        $writer->writeBool($this->cursor !== null);
        $this->cursor?->encode($writer, $version);
        $writer->writeTaggedFields($this->unknownTags);
    }
}

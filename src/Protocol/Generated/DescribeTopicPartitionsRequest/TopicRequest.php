<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeTopicPartitionsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class TopicRequest implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $unknownTags);
    }
}

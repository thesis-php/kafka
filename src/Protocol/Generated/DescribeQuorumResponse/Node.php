<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeQuorumResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class Node implements Encodable, Decodable
{
    /**
     * @param int $nodeId The ID of the associated node.
     * @param list<Node\Listener> $listeners The listeners of this controller.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $nodeId,
        public array $listeners,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->nodeId);
        $writer->writeCompactArrayLength(\count($this->listeners));
        foreach ($this->listeners as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $nodeId = $reader->readInt32();
        $listenersCount = $reader->readCompactArrayLength();
        $listeners = [];
        for ($listenersIndex = 0; $listenersIndex < $listenersCount; ++$listenersIndex) {
            $item = Node\Listener::decode($reader, $version);
            $listeners[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($nodeId, $listeners, $unknownTags);
    }
}

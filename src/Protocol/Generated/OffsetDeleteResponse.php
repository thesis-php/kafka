<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;

/**
 * @api
 * @generated
 */
final readonly class OffsetDeleteResponse implements Response
{
    /**
     * @param int $errorCode The top-level error code, or 0 if there was no error.
     * @param int $throttleTimeMs The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
     * @param list<OffsetDeleteResponse\OffsetDeleteResponseTopic> $topics The responses for each topic.
     */
    public function __construct(
        public int $errorCode,
        public int $throttleTimeMs,
        public array $topics,
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $throttleTimeMs = $reader->readInt32();
        $topicsCount = $reader->readArrayLength();
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = OffsetDeleteResponse\OffsetDeleteResponseTopic::decode($reader, $version);
            $topics[] = $item;
        }
        return new self($errorCode, $throttleTimeMs, $topics);
    }
}

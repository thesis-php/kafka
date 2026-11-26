<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Thesis\Kafka\Consumer;

/**
 * A batch in the buffer of the group consumer, with the session that fetched it.
 *
 * @internal
 */
final readonly class Delivery
{
    /**
     * @param non-empty-list<Consumer\Record> $records
     */
    public function __construct(
        public GroupSession $session,
        public array $records,
    ) {}
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Cancellation;
use Amp\NullCancellation;

/**
 * @api
 */
interface ConsumerSession extends ConsumerContext
{
    /**
     * Commit progress to the group coordinator. Accepts a single record
     * or a list of records; for each (topic, partition) involved, the
     * committed offset is set to the highest record offset + 1 (the next
     * offset to read).
     *
     * Because offsets are linear, committing implies every record up to
     * that offset was processed — make sure you've handled the full
     * prefix, not a sparse subset, or the gaps will be treated as done.
     *
     * Group mode only.
     *
     * @param Consumer\Record|non-empty-list<Consumer\Record> $records
     * @throws Exception\NoGroupConfigured
     */
    public function commit(
        Consumer\Record|array $records,
        Cancellation $cancellation = new NullCancellation(),
    ): void;
}

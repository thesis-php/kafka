<?php

declare(strict_types=1);

namespace Thesis\Kafka;

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
     * @throws Exception
     */
    public function commitRecords(Consumer\Record|array $records): void;

    /**
     * Commit every partition's delivered position — the highest offset the
     * client has handed you since the last commit, across all owned
     * partitions.
     *
     * Unlike {@see commitRecords()}, you pass nothing: the client already
     * tracks how far consumption has advanced per partition, so there's no
     * need to hold on to the records. That also makes
     * this the cheaper call to reach for: instead of committing after every
     * batch, let progress accumulate and call this periodically; each call
     * folds many delivered records into a single commit. Offsets already
     * committed are skipped, so calling it when nothing new arrived is a no-op.
     *
     * Group mode only.
     *
     * @throws Exception
     */
    public function commitUncommitted(): void;
}

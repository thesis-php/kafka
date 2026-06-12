<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\NullCancellation;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster\Metadata;

/**
 * @internal
 */
interface MetadataProvider
{
    /**
     * Resolve the current metadata for a topic: its partitions, their
     * leaders and replicas.
     *
     * Callers treat this as the source of truth and need not know whether
     * the result came from cache or a fresh broker round-trip — the
     * implementation decides, refreshing when the topic is unknown or its
     * cached view has gone stale, and serving from cache otherwise.
     * Concurrent calls for the same topic resolve to a single refresh.
     *
     * @param non-empty-string $topic
     * @throws Exception\UnknownTopic the broker reports no such topic
     * @throws Exception
     */
    public function topic(
        string $topic,
        Cancellation $cancellation = new NullCancellation(),
    ): Metadata\TopicMetadata;
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Thesis\Kafka\Acks;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class ProducerConfig
{
    private const int DEFAULT_LINGER_MS = 5;
    private const int DEFAULT_BATCH_SIZE = 16384; // 16Kb
    private const int DEFAULT_PRODUCE_TIMEOUT_SECS = 30;

    public TimeSpan $linger;

    public TimeSpan $produceTimeout;

    /**
     * @param ?non-empty-string $defaultTopic
     * @param positive-int $batchSize
     */
    public function __construct(
        public ?string $defaultTopic = null,
        ?TimeSpan $linger = null,
        ?TimeSpan $produceTimeout = null,
        public int $batchSize = self::DEFAULT_BATCH_SIZE,
        public Acks $acks = Acks::All,
    ) {
        $this->linger = $linger ?? TimeSpan::fromMilliseconds(self::DEFAULT_LINGER_MS);
        $this->produceTimeout = $produceTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_PRODUCE_TIMEOUT_SECS);
    }
}

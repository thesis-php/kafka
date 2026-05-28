<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Crc32;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class Config
{
    public const int DEFAULT_LINGER_MS = 5;
    public const int DEFAULT_BATCH_SIZE = 16384; // 16Kb
    public const int DEFAULT_PRODUCE_TIMEOUT_SECS = 30;
    public const int DEFAULT_PRODUCE_ATTEMPTS = 10;

    public TimeSpan $linger;

    public TimeSpan $produceTimeout;

    /** @var -1|positive-int */
    public int $batchSize;

    /**
     * @param ?non-empty-string $defaultTopic
     * @param positive-int $produceAttempts
     * @param positive-int $batchSize
     */
    public function __construct(
        public ?string $defaultTopic = null,
        ?TimeSpan $linger = null,
        ?TimeSpan $produceTimeout = null,
        public int $produceAttempts = self::DEFAULT_PRODUCE_ATTEMPTS,
        int $batchSize = self::DEFAULT_BATCH_SIZE,
        public Acks $acks = Acks::All,
        public Compressor $compressor = Compressor\None::Compressor,
        public Crc32 $crc32 = new Crc32\Native(),
        public Partitioner $partitioner = new Partitioner\HashPartitioner(),
        public bool $manualFlush = false,
    ) {
        $linger ??= TimeSpan::fromMilliseconds(self::DEFAULT_LINGER_MS);
        if ($manualFlush) {
            $linger = TimeSpan::fromMilliseconds(0);
            $batchSize = -1;
        }

        $this->linger = $linger;
        $this->produceTimeout = $produceTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_PRODUCE_TIMEOUT_SECS);
        $this->batchSize = $batchSize;
    }
}

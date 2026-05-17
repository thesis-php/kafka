<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

use Thesis\Kafka\Acks;
use Thesis\Kafka\Compressor;
use Thesis\Kafka\Crc32;
use Thesis\Kafka\Partitioner;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class ProducerConfig
{
    private const int DEFAULT_LINGER_MS = 5;
    private const int DEFAULT_BATCH_SIZE = 16384; // 16Kb
    private const int DEFAULT_PRODUCE_TIMEOUT_SECS = 30;
    private const int DEFAULT_PRODUCE_ATTEMPTS = 10;

    public TimeSpan $linger;

    public TimeSpan $produceTimeout;

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
        public int $batchSize = self::DEFAULT_BATCH_SIZE,
        public Acks $acks = Acks::All,
        public ?Compressor $compressor = null,
        public Crc32 $crc32 = new Crc32\Native(),
        public Partitioner $partitioner = new Partitioner\HashPartitioner(),
    ) {
        $this->linger = $linger ?? TimeSpan::fromMilliseconds(self::DEFAULT_LINGER_MS);
        $this->produceTimeout = $produceTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_PRODUCE_TIMEOUT_SECS);
    }
}

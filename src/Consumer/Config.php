<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Crc32;
use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class Config
{
    private const int DEFAULT_MAX_WAIT_SECS = 5;
    private const int DEFAULT_MIN_BYTES = 1;
    private const int DEFAULT_MAX_BYTES = 50 << 20; // 50 MB
    private const int DEFAULT_MAX_PARTITION_BYTES = 1 << 20; // 1 MB
    private const int DEFAULT_PREFETCH_BUFFER_SIZE = 4;
    private const int DEFAULT_POLL_TIMEOUT_SECS = 1;

    public Compressor\Registry $decompressors;

    public TimeSpan $maxWait;

    public TimeSpan $pollTimeout;

    /**
     * @param positive-int $minBytes
     * @param positive-int $maxBytes
     * @param positive-int $maxPartitionBytes
     */
    public function __construct(
        ?Compressor\Registry $decompressors = null,
        public Crc32 $crc32 = new Crc32\Native(),
        ?TimeSpan $maxWait = null,
        ?TimeSpan $pollTimeout = null,
        public int $minBytes = self::DEFAULT_MIN_BYTES,
        public int $maxBytes = self::DEFAULT_MAX_BYTES,
        public int $maxPartitionBytes = self::DEFAULT_MAX_PARTITION_BYTES,
        public AutoOffsetReset $autoOffsetReset = AutoOffsetReset::Latest,
        public IsolationLevel $isolationLevel = IsolationLevel::ReadUncommitted,
        public int $prefetchBufferSize = self::DEFAULT_PREFETCH_BUFFER_SIZE,
        public ?GroupConfig $group = null,
    ) {
        $this->decompressors = $decompressors ?? Compressor\Registry::available();
        $this->maxWait = $maxWait ?? TimeSpan::fromSeconds(self::DEFAULT_MAX_WAIT_SECS);
        $this->pollTimeout = $pollTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_POLL_TIMEOUT_SECS);
    }
}

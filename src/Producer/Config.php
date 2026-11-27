<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Crc32;
use Thesis\Kafka\Exception\InvalidConfiguration;
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
    public const int DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS = 1;

    public TimeSpan $linger;

    /**
     * The time that the broker can wait for the replicas. It is sent in each
     * Produce request. One attempt waits for an answer at most this time plus
     * {@see \Thesis\Kafka\Config::$requestTimeout}. An attempt without an
     * answer in this time is retried like a lost connection.
     */
    public TimeSpan $produceTimeout;

    /**
     * @param ?non-empty-string $defaultTopic
     * @param positive-int $produceAttempts The number of attempts to send a batch, also after a timeout.
     *                                      When the last attempt gets no answer, the records fail with
     *                                      {@see \Thesis\Kafka\Exception\ProduceTimedOut}. Without
     *                                      $idempotent, a retry can write the batch twice.
     * @param positive-int $batchSize
     * @param bool $idempotent Requires acks=All — the broker can only guarantee
     *                         de-duplication for durably-replicated writes.
     * @param positive-int $maxInflightProduceRequest Maximum number of in-flight produce batches per partition. Higher
     *                                          values pipeline responses for throughput, but only the idempotent
     *                                          producer can keep ordering across them.
     * @throws InvalidConfiguration
     */
    public function __construct(
        public ?string $defaultTopic = null,
        ?TimeSpan $linger = null,
        ?TimeSpan $produceTimeout = null,
        public int $produceAttempts = self::DEFAULT_PRODUCE_ATTEMPTS,
        public int $batchSize = self::DEFAULT_BATCH_SIZE,
        public Acks $acks = Acks::All,
        public Compressor $compressor = Compressor\None::Compressor,
        public Crc32 $crc32 = new Crc32\Native(),
        public Partitioner $partitioner = new Partitioner\HashPartitioner(),
        public bool $manualFlush = false,
        public int $maxInflightProduceRequest = self::DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS,
        public bool $idempotent = false,
    ) {
        if ($idempotent && $acks !== Acks::All) {
            throw new InvalidConfiguration('Idempotent producer requires Acks::All');
        }

        if ($idempotent && $maxInflightProduceRequest !== 1) {
            throw new InvalidConfiguration(
                'Idempotent producer requires $maxInflightProduceRequest=1',
            );
        }

        $linger ??= TimeSpan::fromMilliseconds(self::DEFAULT_LINGER_MS);
        $this->linger = $linger;
        $this->produceTimeout = $produceTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_PRODUCE_TIMEOUT_SECS);
    }
}

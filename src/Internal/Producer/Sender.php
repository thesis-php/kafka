<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Producer;

use Amp\Future;
use Thesis\Kafka\Config\ProducerConfig;
use Thesis\Kafka\Internal\Cluster;
use function Amp\async;

/**
 * @internal
 */
final class Sender
{
    /** @var ?Future<null> */
    private ?Future $loop = null;

    /** @var array<non-empty-string, Future<null>> */
    private array $writes = [];

    /** @var non-empty-string */
    private string $writeId = 'a';

    /**
     * @param \Traversable<array-key, ReadyBatch> $batches
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly ProducerConfig $config,
        private readonly \Traversable $batches,
    ) {}

    public function start(): void
    {
        $this->loop ??= $this->doStart();
    }

    public function stop(): void
    {
        $futures = array_values($this->writes);
        $this->writes = [];

        if ($this->loop !== null) {
            $futures[] = $this->loop;
            $this->loop = null;
        }

        Future\awaitAll($futures);
    }

    /**
     * @return Future<null>
     */
    private function doStart(): Future
    {
        $cluster = $this->cluster;
        $config = $this->config;
        $batches = $this->batches;
        $writes = &$this->writes;
        $writeId = &$this->writeId;

        /** @var Future<null> */
        return async(static function () use (
            $cluster,
            $config,
            $batches,
            &$writes,
            &$writeId,
        ): void {
            $producer = new Sender\BatchProducer(
                $cluster,
                $config,
            );

            foreach ($batches as $batch) {
                $idx = $writeId;
                $writeId = str_increment($writeId);
                $writes[$idx] = async($producer->produce(...), $batch)->finally(static function () use (
                    $idx,
                    &$writes,
                ): void {
                    unset($writes[$idx]);
                });
            }
        });
    }
}

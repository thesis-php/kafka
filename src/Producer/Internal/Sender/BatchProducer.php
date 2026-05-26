<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Sender;

use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\NoAvailablePartitionLeader;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Exception\UnknownTopic;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Producer;
use Thesis\Kafka\Producer\Internal\ReadyBatch;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;

/**
 * @internal
 */
final readonly class BatchProducer
{
    public function __construct(
        private Cluster $cluster,
        private Producer\Config $config,
    ) {}

    public function produce(ReadyBatch $batch): void
    {
        $attempt = 0;

        while ($attempt++ < $this->config->produceAttempts) {
            try {
                $leaderId = $this->partitionLeader($batch->topic, $batch->partition);
                $request = $this->createRequest($batch);
                $response = $this->cluster->request(
                    $request,
                    reply: $this->config->acks === Producer\Acks::None ? Connection\Reply::None : Connection\Reply::Expected,
                    brokerId: $leaderId,
                );

                // No response (Acks::None), no broker-assigned offset/timestamp. We
                // still complete the futures so the caller knows the write
                // was at least placed on the wire.
                if ($response === null) {
                    $batch->complete(offset: null, timestamp: null);
                    return;
                }

                $partition = self::findPartitionResponse($response, $batch);

                $error = ErrorCode::from($partition->errorCode);

                if ($error === ErrorCode::None) {
                    $batch->complete(
                        offset: $partition->baseOffset, // @phpstan-ignore argument.type
                        timestamp: $partition->logAppendTimeMs ?? null, // @phpstan-ignore argument.type
                    );

                    return;
                }

                if (!$error->retriable() || $attempt >= $this->config->produceAttempts) {
                    $batch->fail(new \RuntimeException(
                        "Produce failed for {$batch->topic}-{$batch->partition}: {$error->name}.",
                    ));

                    return;
                }

                $this->cluster->refreshMetadata([$batch->topic]);
            } catch (\Throwable $e) {
                if ($attempt >= $this->config->produceAttempts) {
                    $batch->fail($e);
                    return;
                }
            }
        }
    }

    /**
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @return non-negative-int
     * @throws Exception
     */
    private function partitionLeader(
        string $topic,
        int $partition,
    ): int {
        $topicMetadata = $this->cluster->metadata()->topics[$topic]
            ?? $this->cluster->refreshMetadata([$topic])->topics[$topic]
            ?? throw new UnknownTopic($topic);

        $partitionMetadata = $topicMetadata->partitions[$partition]
            ?? throw new UnknownPartition($topic, $partition);

        if ($partitionMetadata->leader < 0) {
            throw new NoAvailablePartitionLeader($topic, $partition);
        }

        return $partitionMetadata->leader;
    }

    private function createRequest(ReadyBatch $ready): Generated\ProduceRequest
    {
        /** @var ?UuidInterface $topicId */
        static $topicId;
        $topicId ??= Uuid::fromString(Uuid::NIL);

        return new Generated\ProduceRequest(
            transactionalId: null,
            acks: $this->config->acks->value,
            timeoutMs: $this->config->produceTimeout->toMilliseconds(),
            topicData: [
                new Generated\ProduceRequest\TopicProduceData(
                    name: $ready->topic,
                    topicId: $topicId,
                    partitionData: [
                        new Generated\ProduceRequest\TopicProduceData\PartitionProduceData(
                            index: $ready->partition,
                            records: $ready->batch->encode(),
                        ),
                    ],
                ),
            ],
        );
    }

    private static function findPartitionResponse(
        Generated\ProduceResponse $response,
        ReadyBatch $batch,
    ): Generated\ProduceResponse\TopicProduceResponse\PartitionProduceResponse {
        foreach ($response->responses as $topic) {
            if ($topic->name !== $batch->topic) {
                continue;
            }

            foreach ($topic->partitionResponses as $partition) {
                if ($partition->index === $batch->partition) {
                    return $partition;
                }
            }
        }

        throw new \RuntimeException(
            "Broker omitted partition {$batch->topic}-{$batch->partition} from response",
        );
    }
}

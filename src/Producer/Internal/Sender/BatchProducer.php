<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\Sender;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\NullCancellation;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\NoAvailablePartitionLeader;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Exception\UnexpectedValue;
use Thesis\Kafka\Exception\UnknownBroker;
use Thesis\Kafka\Exception\UnknownPartition;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\Connection;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\Retry;
use Thesis\Kafka\NetworkException;
use Thesis\Kafka\Producer;
use Thesis\Kafka\Producer\Internal\ReadyBatch;
use Thesis\Kafka\Protocol\Error\PartitionError;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use function Amp\delay;

/**
 * @internal
 */
final readonly class BatchProducer
{
    public function __construct(
        private Cluster $cluster,
        private Producer\Config $config,
        private ContextLogger $logger,
    ) {}

    public function produce(
        ReadyBatch $batch,
        Cancellation $cancellation = new NullCancellation(),
    ): void {
        for ($attempt = 1;; ++$attempt) {
            try {
                $failure = $this->send($batch, $attempt, $cancellation);

                if ($failure === null) {
                    return;
                }
            } catch (CancelledException $e) {
                $batch->fail($e);

                return;
            } catch (\Throwable $e) {
                if (!self::retriable($e)) {
                    // @infection-ignore-all
                    $this->logger->error('produce failed permanently', [
                        'topic' => $batch->topic,
                        'partition' => $batch->partition,
                        'attempt' => $attempt,
                        'exception' => $e,
                    ]);

                    $batch->fail($e);

                    return;
                }

                $failure = $e;
            }

            if ($attempt >= $this->config->produceAttempts) {
                // @infection-ignore-all
                $this->logger->error('produce failed permanently', [
                    'topic' => $batch->topic,
                    'partition' => $batch->partition,
                    'attempt' => $attempt,
                    'exception' => $failure,
                ]);

                $batch->fail($failure);

                return;
            }

            // @infection-ignore-all
            $this->logger->warning('produce failed, retrying', [
                'topic' => $batch->topic,
                'partition' => $batch->partition,
                'attempt' => $attempt,
                'maxAttempts' => $this->config->produceAttempts,
                'exception' => $failure,
            ]);

            // The leader may change, or may not know a new topic yet.
            // Wait a little and ask for the new leader before the next attempt.
            try {
                delay(Retry\backoff($attempt), cancellation: $cancellation);
                $this->cluster->refreshMetadata([$batch->topic], $cancellation);
            } catch (CancelledException $e) {
                $batch->fail($e);

                return;
            } catch (\Throwable $e) {
                // The next attempt finds out if the broker is still gone.
                // @infection-ignore-all
                $this->logger->warning('metadata refresh failed', [
                    'topic' => $batch->topic,
                    'partition' => $batch->partition,
                    'attempt' => $attempt,
                    'exception' => $e,
                ]);
            }
        }
    }

    /**
     * Sends the batch once.
     *
     * @param positive-int $attempt
     * @return ?ProtocolError the error of the broker that the next attempt can fix,
     *                        or null when the batch is done
     * @throws \Throwable
     */
    private function send(ReadyBatch $batch, int $attempt, Cancellation $cancellation): ?ProtocolError
    {
        [$leaderId, $topicId] = $this->partitionLeader(
            $batch->topic,
            $batch->partition,
            $cancellation,
        );

        // @infection-ignore-all
        $this->logger->debug('producing batch', [
            'topic' => $batch->topic,
            'partition' => $batch->partition,
            'recordCount' => \count($batch->futures),
            'baseSequence' => $batch->batch->baseSequence,
            'attempt' => $attempt,
        ]);

        $response = $this->cluster->request(
            request: $this->createRequest($batch, $topicId),
            cancellation: $cancellation,
            reply: $this->config->acks === Producer\Acks::None ? Connection\Reply::None : Connection\Reply::Expected,
            brokerId: $leaderId,
        );

        // No response (Acks::None), no broker-assigned offset/timestamp. We
        // still complete the futures so the caller knows the write
        // was at least placed on the wire.
        if ($response === null) {
            $batch->complete(offset: null, timestamp: null);

            return null;
        }

        $partition = self::findPartitionResponse($response, $batch, $topicId);

        $error = ErrorCode::tryFrom($partition->errorCode) ?? ErrorCode::UnknownServerError;

        if ($error === ErrorCode::None) {
            $batch->complete(
                offset: $partition->baseOffset, // @phpstan-ignore argument.type
                timestamp: $partition->logAppendTimeMs, // @phpstan-ignore argument.type
            );

            // @infection-ignore-all
            $this->logger->debug('batch acknowledged', [
                'topic' => $batch->topic,
                'partition' => $batch->partition,
                'baseOffset' => $partition->baseOffset,
            ]);

            return null;
        }

        $failure = new ProtocolError(new Errors([
            new PartitionError(
                topic: $batch->topic,
                partition: $batch->partition,
                code: $error,
                message: $partition->errorMessage,
            ),
        ]));

        if (!$error->retriable()) {
            // @infection-ignore-all
            $this->logger->error('produce failed permanently', [
                'topic' => $batch->topic,
                'partition' => $batch->partition,
                'error' => $error->name,
                'attempt' => $attempt,
            ]);

            $batch->fail($failure);

            return null;
        }

        return $failure;
    }

    /**
     * Tells whether a new attempt can fix the failure: the connection was
     * lost, or the metadata is old.
     */
    private static function retriable(\Throwable $e): bool
    {
        return $e instanceof NetworkException
            || $e instanceof UnknownBroker
            || $e instanceof NoAvailablePartitionLeader;
    }

    /**
     * Returns the leader of the partition and the id of the topic.
     * Brokers without topic ids give a nil id.
     *
     * @param non-empty-string $topic
     * @param non-negative-int $partition
     * @return array{non-negative-int, UuidInterface}
     * @throws Exception
     */
    private function partitionLeader(
        string $topic,
        int $partition,
        Cancellation $cancellation = new NullCancellation(),
    ): array {
        $topicMetadata = $this->cluster->topic($topic, $cancellation);

        $partitionMetadata = $topicMetadata->partitions[$partition]
            ?? throw new UnknownPartition($topic, $partition);

        if ($partitionMetadata->leader < 0) {
            throw new NoAvailablePartitionLeader($topic, $partition);
        }

        return [$partitionMetadata->leader, $topicMetadata->topicId];
    }

    /**
     * Sets both the name and the id. Produce v13+ sends only the id, older versions only the name.
     */
    private function createRequest(ReadyBatch $ready, UuidInterface $topicId): Generated\ProduceRequest
    {
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
        UuidInterface $topicId,
    ): Generated\ProduceResponse\TopicProduceResponse\PartitionProduceResponse {
        foreach ($response->responses as $topic) {
            // Produce v13+ responses have the topic id and an empty name.
            $same = $topic->name === '' ? $topic->topicId->equals($topicId) : $topic->name === $batch->topic;

            if (!$same) {
                continue;
            }

            foreach ($topic->partitionResponses as $partition) {
                if ($partition->index === $batch->partition) {
                    return $partition;
                }
            }
        }

        throw new UnexpectedValue(
            "Broker omitted partition {$batch->topic}-{$batch->partition} from the Produce response.",
        );
    }
}

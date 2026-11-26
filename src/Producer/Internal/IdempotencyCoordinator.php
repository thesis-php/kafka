<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\Retry;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\TopicPartition;

/**
 * @internal
 */
final class IdempotencyCoordinator
{
    private ?Idempotency $idempotency = null;

    /**
     * Non-null while an {@see Generated\InitProducerIdRequest} request is in flight, so
     * concurrent first-produce calls share one round-trip.
     *
     * @var ?Future<Idempotency>
     */
    private ?Future $initialize = null;

    /**
     * Next sequence number per partition. Sealing a batch reserves a
     * contiguous range starting here and advances by the record count.
     *
     * @var TopicPartition\Map<non-negative-int>
     */
    private TopicPartition\Map $sequences;

    public function __construct(
        private readonly Cluster $cluster,
        private readonly ContextLogger $logger,
    ) {
        $this->sequences = new TopicPartition\Map();
    }

    /**
     * Return the producer's idempotent identity, obtaining it from the
     * broker on first call. Subsequent calls return the same instance;
     * concurrent first calls await a single {@see Generated\InitProducerIdRequest}.
     *
     * @throws \Throwable
     */
    public function initialize(Cancellation $cancellation): Idempotency
    {
        if ($this->idempotency !== null) {
            return $this->idempotency;
        }

        if ($this->initialize !== null) {
            return $this->initialize->await($cancellation);
        }

        /** @var DeferredFuture<Idempotency> $deferred */
        $deferred = new DeferredFuture();
        $this->initialize = $deferred->getFuture();

        try {
            $idempotency = $this->requestProducerId($cancellation);
            $this->idempotency = $idempotency;
            // @infection-ignore-all
            $this->logger->info('producer id obtained', [
                'producerId' => $idempotency->producerId,
                'producerEpoch' => $idempotency->producerEpoch,
            ]);

            $deferred->complete($idempotency);

            return $idempotency;
        } catch (\Throwable $e) {
            // @infection-ignore-all
            $this->logger->critical('failed to obtain producer id', [
                'exception' => $e,
            ]);

            $deferred->error($e);

            throw $e;
        } finally {
            $this->initialize = null;
        }
    }

    public function sequenceFor(TopicPartition $tp): int
    {
        return $this->sequences[$tp] ?? 0;
    }

    /**
     * Reserve a contiguous range of $count sequence numbers for a
     * partition, returning the base (first). Records in the sealed batch
     * map to base .. base + count - 1.
     *
     * @param positive-int $count
     */
    public function reserveSequence(TopicPartition $tp, int $count): void
    {
        $this->sequences[$tp] = ($this->sequences[$tp] ?? 0) + $count;
    }

    /**
     * @throws Exception
     * @throws CancelledException
     */
    private function requestProducerId(Cancellation $cancellation): Idempotency
    {
        $request = new Generated\InitProducerIdRequest(
            transactionalId: null, // null = idempotent, not transactional
            transactionTimeoutMs: 0, // unused without a transactional id
            producerId: -1,
            producerEpoch: -1,
            // KIP-939 2PC — transactional only
            enable2Pc: false,
            keepPreparedTxn: false,
        );

        $response = $this->cluster->request(
            request: $request,
            cancellation: $cancellation,
            retry: new Retry\Operation(
                static fn(Generated\InitProducerIdResponse $response): ErrorCode => ErrorCode::tryFrom($response->errorCode) ?? ErrorCode::UnknownServerError,
                // @infection-ignore-all
                logger: $this->logger->with([
                    'component' => 'retry',
                    'operation' => 'initProducerId',
                ]),
            ),
        );

        $errors = Errors::for($response);
        if (!$errors->empty()) {
            throw new ProtocolError($errors);
        }

        $producerId = $response->producerId;
        \assert($producerId >= 0);

        $producerEpoch = $response->producerEpoch;
        \assert($producerEpoch >= 0);

        return new Idempotency(
            producerId: $producerId,
            producerEpoch: $producerEpoch,
        );
    }
}

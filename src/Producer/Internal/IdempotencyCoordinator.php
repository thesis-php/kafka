<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
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
use function Amp\async;

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
     * The numbers of the next batch of each partition.
     *
     * @var TopicPartition\Map<BatchSequence>
     */
    private TopicPartition\Map $sequences;

    /**
     * @param Cancellation $closing ends the request when the producer closes
     */
    public function __construct(
        private readonly Cluster $cluster,
        private readonly ContextLogger $logger,
        private readonly Cancellation $closing,
    ) {
        $this->sequences = new TopicPartition\Map();
    }

    /**
     * Return the producer's idempotent identity, obtaining it from the
     * broker on first call. Subsequent calls send no request;
     * concurrent first calls await a single {@see Generated\InitProducerIdRequest}.
     * Each caller waits with its own cancellation. After a failed request
     * the next call sends a new one.
     *
     * @throws \Throwable
     */
    public function initialize(Cancellation $cancellation): Idempotency
    {
        if ($this->idempotency !== null) {
            return $this->idempotency;
        }

        $this->initialize ??= $this->startInitialize();

        return $this->initialize->await($cancellation);
    }

    /**
     * Gives the numbers to the next batch of the partition. Call it only
     * when the partition has no batch in flight. Then the partition can
     * move to the newest epoch and start again at sequence 0.
     *
     * @param non-negative-int $count the number of records in the batch
     */
    public function next(TopicPartition $tp, int $count): BatchSequence
    {
        $idempotency = $this->idempotency;
        \assert($idempotency !== null, 'a batch is sent only after the producer id is known');

        $next = $this->sequences[$tp] ?? null;
        if ($next?->producerEpoch !== $idempotency->producerEpoch) {
            $next = new BatchSequence(
                producerId: $idempotency->producerId,
                producerEpoch: $idempotency->producerEpoch,
                baseSequence: 0,
            );
        }

        $this->sequences[$tp] = new BatchSequence(
            producerId: $next->producerId,
            producerEpoch: $next->producerEpoch,
            baseSequence: $next->baseSequence + $count,
        );

        return $next;
    }

    /**
     * Raises the epoch after a batch failed for good. The broker may have
     * written the batch or not, so the old sequence of the partition is not
     * safe any more. The broker accepts a higher epoch with sequence 0.
     */
    public function raiseEpoch(): void
    {
        $idempotency = $this->idempotency;
        \assert($idempotency !== null, 'a batch is sent only after the producer id is known');

        $this->idempotency = new Idempotency(
            producerId: $idempotency->producerId,
            producerEpoch: $idempotency->producerEpoch + 1,
        );

        // @infection-ignore-all
        $this->logger->warning('producer epoch raised', [
            'producerId' => $idempotency->producerId,
            'producerEpoch' => $idempotency->producerEpoch + 1,
        ]);
    }

    /**
     * Starts the request that concurrent callers share. It does not use the
     * cancellation of the first caller.
     *
     * @return Future<Idempotency>
     */
    private function startInitialize(): Future
    {
        /** @var Future<Idempotency> */
        return async(function (): Idempotency {
            try {
                $idempotency = $this->requestProducerId($this->closing);
                $this->idempotency = $idempotency;
                // @infection-ignore-all
                $this->logger->info('producer id obtained', [
                    'producerId' => $idempotency->producerId,
                    'producerEpoch' => $idempotency->producerEpoch,
                ]);

                return $idempotency;
            } catch (\Throwable $e) {
                // @infection-ignore-all
                $this->logger->critical('failed to obtain producer id', [
                    'exception' => $e,
                ]);

                throw $e;
            } finally {
                $this->initialize = null;
            }
        })
            // A request that no caller waits for any more must not report an unhandled error.
            ->ignore();
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

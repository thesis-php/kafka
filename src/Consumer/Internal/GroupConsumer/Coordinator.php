<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\Internal\GroupConsumer;

use Amp\Cancellation;
use Amp\CancelledException;
use Thesis\Kafka\Exception;
use Thesis\Kafka\Internal\Cluster;
use Thesis\Kafka\Internal\ContextLogger;
use Thesis\Kafka\Internal\RequestTimeout;
use Thesis\Kafka\Internal\Retry;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Errors;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Time\TimeSpan;
use function Amp\delay;

/**
 * @internal
 */
final class Coordinator
{
    /** @var ?non-negative-int */
    private ?int $brokerId = null;

    /**
     * @param non-empty-string $groupId
     * @param positive-int $maxAttempts
     */
    public function __construct(
        private readonly string $groupId,
        private readonly Cluster $cluster,
        private readonly ContextLogger $logger,
        private readonly TimeSpan $requestTimeout,
        private readonly int $maxAttempts = 5,
    ) {}

    /**
     * Each attempt has its own time limit, so a lost response cannot block the
     * caller. The cancellation of the caller ends all attempts.
     *
     * @template T of Generated\Response
     * @param Generated\Request<T> $request
     * @param ?TimeSpan $hold how long the broker may hold the request before it answers.
     *                        One attempt gets this time and the request timeout of the client.
     * @return T
     * @throws Exception
     * @throws CancelledException
     * @throws \Throwable
     */
    public function request(
        Generated\Request $request,
        ?Cancellation $cancellation = null,
        ?TimeSpan $hold = null,
    ): Generated\Response {
        $timeout = $hold === null ? $this->requestTimeout : $this->requestTimeout->add($hold);
        $seconds = $timeout->toSeconds(3);

        $backoff = static fn(int $attempt) => delay(Retry\backoff($attempt), cancellation: $cancellation); // @phpstan-ignore argument.type

        for ($attempt = 1;; ++$attempt) {
            $brokerId = $this->discover($cancellation);

            try {
                /** @var T $response */
                $response = $this->cluster->request(
                    $request,
                    RequestTimeout::start($seconds, $cancellation),
                    brokerId: $brokerId,
                );
            } catch (Exception\UnknownBroker $e) {
                // The coordinator's node isn't in the routing table yet: FindCoordinator
                // can name a broker no metadata load has taught us how to reach. Learn the
                // broker set, then retry the same (still valid) coordinator.
                if ($attempt >= $this->maxAttempts) {
                    throw $e;
                }

                $this->cluster->refreshMetadata(cancellation: $cancellation);
                continue;
            } catch (\Throwable $e) {
                // The caller has stopped waiting. This says nothing about the coordinator.
                if ($cancellation?->isRequested() === true) {
                    throw $e;
                }

                // Connection to the coordinator failed or the attempt ran out of time: it may have moved.
                $this->invalidate($e->getMessage());

                if ($attempt >= $this->maxAttempts) {
                    throw $e;
                }

                $backoff($attempt);
                continue;
            }

            $errors = Errors::for($response);

            if (!$errors->empty() && $attempt >= $this->maxAttempts) {
                return $response;
            }

            if ($errors->has(ErrorCode::NotCoordinator, ErrorCode::CoordinatorNotAvailable, ErrorCode::CoordinatorLoadInProgress)) {
                if (!$errors->has(ErrorCode::CoordinatorLoadInProgress)) {
                    $this->invalidate((string) $errors);
                }

                $backoff($attempt);
                continue;
            }

            return $response;
        }
    }

    /**
     * @return non-negative-int
     */
    private function discover(?Cancellation $cancellation = null): int
    {
        // Each caller finds the coordinator with its own cancellation. Two callers
        // may both send FindCoordinator. This is rare and harmless.
        return $this->brokerId ??= $this->findCoordinator($cancellation);
    }

    /**
     * @return non-negative-int
     * @throws Exception
     * @throws CancelledException
     */
    private function findCoordinator(?Cancellation $cancellation = null): int
    {
        for ($attempt = 1;; ++$attempt) {
            $response = $this->cluster->request(
                new Generated\FindCoordinatorRequest(
                    key: $this->groupId,
                    keyType: 0,
                    coordinatorKeys: [
                        $this->groupId,
                    ],
                ),
                RequestTimeout::start($this->requestTimeout->toSeconds(3), $cancellation),
            );

            $coordinator = $response->coordinators[0] ?? null;

            if ($coordinator !== null) {
                // Modern (v4+): per-entry node + error in the coordinators list.
                $brokerId = $coordinator->nodeId;
                $host = $coordinator->host;
                $port = $coordinator->port;
            } else {
                // Legacy (v3 and below): single coordinator at the top level.
                $brokerId = $response->nodeId;
                $host = $response->host;
                $port = $response->port;
            }

            $errors = Errors::for($response);

            if ($errors->empty()) {
                \assert($brokerId >= 0);

                // @infection-ignore-all
                $this->logger->info('group coordinator discovered', [
                    'nodeId' => $brokerId,
                    'host' => $host,
                    'port' => $port,
                ]);

                return $brokerId;
            }

            if ($errors->has(ErrorCode::CoordinatorNotAvailable, ErrorCode::CoordinatorLoadInProgress)) {
                // @infection-ignore-all
                $this->logger->warning('group coordinator retriable error', [
                    'error' => (string) $errors,
                    'attempt' => $attempt,
                ]);

                delay(Retry\backoff($attempt), cancellation: $cancellation);
                continue;
            }

            throw new Exception\ProtocolError($errors);
        }
    }

    private function invalidate(?string $reason): void
    {
        [$brokerId, $this->brokerId] = [$this->brokerId, null];

        if ($brokerId !== null) {
            // @infection-ignore-all
            $this->logger->notice('group coordinator invalidated', [
                'nodeId' => $brokerId,
                'reason' => $reason,
            ]);
        }
    }
}

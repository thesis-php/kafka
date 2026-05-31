<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Thesis\Kafka\Exception;
use Thesis\Kafka\NetworkException;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use function Amp\delay;

/**
 * @internal
 * @template T of Generated\Response
 */
final readonly class Retry
{
    /**
     * @param \Closure(T): ErrorCode $errorOf
     * @param positive-int $maxAttempts
     */
    public function __construct(
        public \Closure $errorOf,
        public int $maxAttempts = 5,
    ) {}

    /**
     * @internal
     * @param \Closure(): T $operation
     * @return \Closure(): T
     */
    public function do(
        \Closure $operation,
        Cancellation $cancellation,
    ): callable {
        $errorOf = $this->errorOf;
        $maxAttempts = $this->maxAttempts;

        return static function () use (
            $errorOf,
            $maxAttempts,
            $operation,
            $cancellation,
        ): mixed {
            for ($attempt = 1;; ++$attempt) {
                try {
                    $error = $errorOf($response = $operation());

                    if ($error === ErrorCode::None) {
                        return $response;
                    }

                    if (!$error->retriable() || $attempt >= $maxAttempts) {
                        return $response;
                    }
                } catch (Exception $e) {
                    if (!isNetwork($e) || $attempt >= $maxAttempts) {
                        throw $e;
                    }
                }

                delay(
                    backoff($attempt),
                    cancellation: $cancellation,
                );
            }
        };
    }
}

/**
 * Backoff delay in seconds for the given attempt (1-based).
 * Exponential base (doubling per attempt) capped at a ceiling, then
 * jittered to 50–100% of that value.
 *
 * @internal
 * @param positive-int $attempt
 */
function backoff(int $attempt): float
{
    $exp = 100 * (2 ** ($attempt - 1));
    $capped = min($exp, 1000);

    $jitter = $capped * (0.5 + (random_int(0, 1_000) / 1_000) * 0.5);

    return $jitter / 1000;
}

/**
 * @internal
 */
function isNetwork(Exception $e): bool
{
    return $e instanceof NetworkException;
}

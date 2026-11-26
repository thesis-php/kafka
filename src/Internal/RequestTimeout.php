<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\TimeoutCancellation;

/**
 * Creates the request timeout of the client, from {@see \Thesis\Kafka\Config::$requestTimeout}.
 * A connection asks it whether a cancellation is such a timeout. Only this
 * timeout tells the connection that a request waited the full request
 * timeout. A cancellation of the caller tells it nothing like that.
 *
 * @internal
 */
final class RequestTimeout
{
    /**
     * The hrtime() when each request timeout started.
     *
     * @var ?\WeakMap<TimeoutCancellation, int>
     */
    private static ?\WeakMap $startedAt = null;

    public static function start(float $seconds): TimeoutCancellation
    {
        $startedAt = (int) hrtime(true);
        $timeout = new TimeoutCancellation($seconds);

        self::$startedAt ??= new \WeakMap();
        self::$startedAt[$timeout] = $startedAt;

        return $timeout;
    }

    /**
     * Returns the hrtime() when the timeout started, or null when the
     * cancellation is not a request timeout of the client.
     */
    public static function startedAt(Cancellation $cancellation): ?int
    {
        if (!$cancellation instanceof TimeoutCancellation) {
            return null;
        }

        return self::$startedAt[$cancellation] ?? null;
    }
}

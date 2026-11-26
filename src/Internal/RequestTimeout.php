<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;

/**
 * Creates the request timeout of the client, from {@see \Thesis\Kafka\Config::$requestTimeout}
 * or from another limit of a request. A connection asks it whether such a
 * timeout has fired. Only this timeout tells the connection that a request
 * waited the full time. A cancellation of the caller tells it nothing like that.
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

    /**
     * The request timeout inside each cancellation that joins it with the cancellation of a caller.
     *
     * @var ?\WeakMap<CompositeCancellation, TimeoutCancellation>
     */
    private static ?\WeakMap $timeouts = null;

    /**
     * @param ?Cancellation $cancellation the cancellation of the caller, it also ends the request
     */
    public static function start(float $seconds, ?Cancellation $cancellation = null): Cancellation
    {
        $startedAt = (int) hrtime(true);
        $timeout = new TimeoutCancellation($seconds);

        self::$startedAt ??= new \WeakMap();
        self::$startedAt[$timeout] = $startedAt;

        if ($cancellation === null) {
            return $timeout;
        }

        $limited = new CompositeCancellation($cancellation, $timeout);

        self::$timeouts ??= new \WeakMap();
        self::$timeouts[$limited] = $timeout;

        return $limited;
    }

    /**
     * Returns the hrtime() when the timeout started, if the request timeout itself
     * has fired. Returns null when it has not fired, when only the caller has
     * cancelled, or when the cancellation is not a request timeout of the client.
     */
    public static function firedSince(Cancellation $cancellation): ?int
    {
        $timeout = $cancellation instanceof CompositeCancellation
            ? self::$timeouts[$cancellation] ?? null
            : $cancellation;

        if (!$timeout instanceof TimeoutCancellation || !$timeout->isRequested()) {
            return null;
        }

        return self::$startedAt[$timeout] ?? null;
    }
}

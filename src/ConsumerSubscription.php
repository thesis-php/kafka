<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * A handle to a background consume loop started via {@see Client::consume()}.
 * The handler runs on its own, draining records and invoking the
 * callback; this handle lets the caller stop it ({@see ConsumerContext::close()}) or wait
 * for it to finish ({@see join()}).
 *
 * Extends {@see ConsumerContext} — a subscription can be closed but not
 * committed through directly; committing happens inside the handler via
 * the {@see ConsumerSession} passed to it.
 *
 * @api
 */
interface ConsumerSubscription extends ConsumerContext
{
    /**
     * Block until the subscription's background loop finishes — because
     * it was closed, its cancellation fired, or the handler threw. If
     * the loop ended with an error, that error is rethrown here.
     *
     * Use this to keep the caller alive for the lifetime of the
     * subscription, or at shutdown to surface any failure the
     * background loop hit.
     *
     * @throws \Throwable
     */
    public function join(): void;
}

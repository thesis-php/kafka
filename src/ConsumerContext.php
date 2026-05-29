<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * The minimal control surface over an active consumer: stop it. Held by
 * callers that should be able to shut a subscription down but have no
 * business committing or reading directly.
 *
 * @api
 */
interface ConsumerContext
{
    /**
     * Leave the group (if any), stop fetching, release resources.
     * Idempotent.
     *
     * @throws Exception
     */
    public function close(): void;
}

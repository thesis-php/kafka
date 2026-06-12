<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

/**
 * Names the rebalance protocol a consumer group uses. The shared root of
 * all balancer kinds — every protocol, including the server-side KIP-848
 * one, needs a name the coordinator matches members on.
 *
 * @api
 */
interface GroupBalancer
{
    /**
     * Protocol name advertised to the coordinator: "range", "roundrobin",
     * "cooperative-sticky" for the classic protocol, or a server-side
     * strategy name ("uniform", "range") for KIP-848. Members must share a
     * common name for the coordinator to select one.
     *
     * @return non-empty-string
     */
    public function name(): string;
}

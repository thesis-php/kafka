<?php

declare(strict_types=1);

namespace Thesis\Kafka;

/**
 * @api
 */
enum Acks: int
{
    /**
     * Fire-and-forget. The broker does not send a response; the
     * producer considers the record delivered the moment it's
     * written to the socket. Lowest durability, lowest latency.
     */
    case None = 0;

    /**
     * The partition leader acknowledges the write before responding.
     * Followers may or may not have replicated yet. If the leader
     * crashes immediately after acknowledging but before followers
     * catch up, the record may be lost.
     */
    case Leader = 1;

    /**
     * Every in-sync replica acknowledges the write before the leader
     * responds. Highest durability — the record is safe as long as
     * at least one in-sync replica survives. Highest latency,
     * because the leader must wait for the slowest ISR member.
     */
    case All = -1;
}

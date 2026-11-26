<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

/**
 * What a {@see BrokenPeer} does with a connection that it accepts.
 *
 * @internal
 */
enum PeerFault
{
    /**
     * Closes the connection at once.
     */
    case Close;

    /**
     * Reads everything and never answers.
     */
    case Stall;

    /**
     * Answers the first bytes with a frame length of 2 GiB and then sends
     * {@see BrokenPeer::GARBAGE_BYTES} bytes of garbage.
     */
    case Garbage;

    /**
     * Answers ApiVersions with FindCoordinator v3-v4. Then it answers the
     * next request with a frame that ends inside the first varint.
     */
    case CutVarint;

    /**
     * Answers ApiVersions with FindCoordinator v3-v4. Then it answers the
     * next request with a frame whose first varint has 11 bytes.
     */
    case LongVarint;
}

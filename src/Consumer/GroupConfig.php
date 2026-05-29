<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer;

use Thesis\Time\TimeSpan;

/**
 * @api
 */
final readonly class GroupConfig
{
    private const int DEFAULT_SESSION_TIMEOUT_SECS = 45;
    private const int DEFAULT_HEARTBEAT_INTERVAL_SECS = 3;
    private const int DEFAULT_AUTOCOMMIT_INTERVAL_SECS = 5;

    public TimeSpan $sessionTimeout;

    public TimeSpan $heartbeatInterval;

    public TimeSpan $autocommitInterval;

    /**
     * @param non-empty-string $groupId
     */
    public function __construct(
        public string $groupId,
        ?TimeSpan $sessionTimeout = null,
        ?TimeSpan $heartbeatInterval = null,
        ?TimeSpan $autocommitInterval = null,
        public bool $autocommit = false,
    ) {
        $this->sessionTimeout = $sessionTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_SESSION_TIMEOUT_SECS);
        $this->heartbeatInterval = $heartbeatInterval ?? TimeSpan::fromSeconds(self::DEFAULT_HEARTBEAT_INTERVAL_SECS);
        $this->autocommitInterval = $autocommitInterval ?? TimeSpan::fromSeconds(self::DEFAULT_AUTOCOMMIT_INTERVAL_SECS);
    }
}

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
    private const int DEFAULT_REBALANCE_INTERVAL_SECS = 10;

    public TimeSpan $sessionTimeout;

    public TimeSpan $heartbeatInterval;

    public TimeSpan $autocommitInterval;

    public TimeSpan $rebalanceTimeout;

    /** @var non-empty-list<ClassicGroupBalancer> */
    public array $balancers;

    /**
     * @param non-empty-string $groupId
     * @param ?non-empty-string $groupInstanceId
     * @param list<ClassicGroupBalancer> $balancers
     */
    public function __construct(
        public string $groupId,
        public ?string $groupInstanceId = null,
        ?TimeSpan $sessionTimeout = null,
        ?TimeSpan $heartbeatInterval = null,
        ?TimeSpan $autocommitInterval = null,
        ?TimeSpan $rebalanceTimeout = null,
        public bool $autocommit = false,
        array $balancers = [],
    ) {
        $this->sessionTimeout = $sessionTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_SESSION_TIMEOUT_SECS);
        $this->heartbeatInterval = $heartbeatInterval ?? TimeSpan::fromSeconds(self::DEFAULT_HEARTBEAT_INTERVAL_SECS);
        $this->autocommitInterval = $autocommitInterval ?? TimeSpan::fromSeconds(self::DEFAULT_AUTOCOMMIT_INTERVAL_SECS);
        $this->rebalanceTimeout = $rebalanceTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_REBALANCE_INTERVAL_SECS);

        if ($balancers === []) {
            $balancers = [
                new GroupBalancer\RangeGroupBalancer(),
                new GroupBalancer\RoundRobinGroupBalancer(),
            ];
        }

        $this->balancers = $balancers;
    }
}

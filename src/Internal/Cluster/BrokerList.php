<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Cluster;

/**
 * @internal
 * @template-implements \IteratorAggregate<int, Broker>
 */
final class BrokerList implements \IteratorAggregate
{
    /** @var array<int, Broker> */
    public private(set) array $seeds = [];

    public function random(): int
    {
        $brokerId = Broker::NOT_A_BROKER_ID;

        if ($this->seeds !== []) {
            $idx = random_int(0, \count($this->seeds) - 1);
            $brokerId = $this->seeds[$idx]->id ?? Broker::NOT_A_BROKER_ID;
        }

        return $brokerId;
    }

    public function add(Broker $broker): void
    {
        $this->seeds[$broker->id] = $broker;
    }

    public function remove(int $brokerId): void
    {
        unset($this->seeds[$brokerId]);
    }

    public function contains(int $brokerId): bool
    {
        return isset($this->seeds[$brokerId]);
    }

    /**
     * @return list<Broker>
     */
    public function purge(): array
    {
        [$seeds, $this->seeds] = [$this->seeds, []];

        return array_values($seeds);
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->seeds;
    }
}

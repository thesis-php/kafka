<?php

declare(strict_types=1);

namespace Thesis\Kafka\Testing;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Time\TimeSpan;
use function Amp\async;

/**
 * Settings of one test run. Create it with {@see Kafka::connect()}.
 *
 * @internal
 */
final readonly class Setup
{
    private const int DEFAULT_TIMEOUT_SECONDS = 60;

    private TimeSpan $timeout;

    /**
     * @param non-empty-list<non-empty-string> $seeds
     */
    public function __construct(
        private array $seeds,
        private ?SASLConfig $sasl = null,
        ?TimeSpan $timeout = null,
        private LoggerInterface $logger = new NullLogger(),
    ) {
        $this->timeout = $timeout ?? TimeSpan::fromSeconds(self::DEFAULT_TIMEOUT_SECONDS);
    }

    public function sasl(SASLConfig $sasl): self
    {
        return new self($this->seeds, $sasl, $this->timeout, $this->logger);
    }

    /**
     * Sets the time limit of the whole scenario.
     */
    public function timeout(TimeSpan $timeout): self
    {
        return new self($this->seeds, $this->sasl, $timeout, $this->logger);
    }

    /**
     * Sets the logger of the clients that the kit creates.
     */
    public function logger(LoggerInterface $logger): self
    {
        return new self($this->seeds, $this->sasl, $this->timeout, $logger);
    }

    /**
     * Runs the scenario and releases everything it got from the kit.
     * An exception of the scenario leaves this method unchanged.
     *
     * @param \Closure(Kafka): void $scenario
     * @throws \Throwable
     */
    public function test(\Closure $scenario): void
    {
        $config = new Config(seeds: $this->seeds, sasl: $this->sasl);
        $client = new Client($config, $this->logger);
        $resources = new Resources($client);
        $stop = new DeferredCancellation();

        $future = async($scenario, new Kafka($client, $config, $resources, $stop->getCancellation(), $this->logger));
        $timer = new TimeoutCancellation($this->timeout->toSeconds(3));
        $failure = null;

        try {
            $future->await($timer);
        } catch (CancelledException $e) {
            $failure = $e;

            if ($timer->isRequested() && !$future->isComplete()) {
                // The scenario can still run. Stop the waits of the kit and drop its result.
                $stop->cancel();
                $future->ignore();
                $failure = new ScenarioTimedOut($this->timeout);
            }
        } catch (\Throwable $e) {
            $failure = $e;
        }

        $errors = $resources->release();

        // A failure of the scenario is more important than a failure of the cleanup.
        if ($failure !== null) {
            throw $failure;
        }

        if ($errors !== []) {
            throw new CleanupFailed($errors);
        }
    }
}

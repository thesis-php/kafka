<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Thesis\Package;
use Thesis\Time\TimeSpan;

/**
 * @api
 * @template-implements \IteratorAggregate<array-key, Config\Endpoint>
 */
final readonly class Config implements \IteratorAggregate
{
    private const string PACKAGE_NAME = 'thesis/kafka';
    private const string DEFAULT_CLIENT_ID = 'thesis';
    private const string DEFAULT_CLIENT_SOFTWARE_NAME = 'thesis';
    private const int DEFAULT_CONNECT_TIMEOUT_SECS = 5;
    private const int DEFAULT_REQUEST_TIMEOUT_SECS = 30;

    /** @var non-empty-list<Config\Endpoint> */
    public array $seeds;

    public TimeSpan $connectTimeout;

    public TimeSpan $requestTimeout;

    /** @var non-empty-string */
    public string $clientSoftwareVersion;

    /**
     * @param non-empty-list<Config\Endpoint|non-empty-string> $seeds
     * @param non-empty-string $clientId
     * @param non-empty-string $clientSoftwareName
     * @param ?non-empty-string $clientSoftwareVersion
     */
    public function __construct(
        array $seeds,
        public string $clientId = self::DEFAULT_CLIENT_ID,
        ?TimeSpan $connectTimeout = null,
        ?TimeSpan $requestTimeout = null,
        public string $clientSoftwareName = self::DEFAULT_CLIENT_SOFTWARE_NAME,
        ?string $clientSoftwareVersion = null,
        public ?Config\TLSConfig $tls = null,
        public ?Config\SASLConfig $sasl = null,
    ) {
        $this->seeds = array_map(Config\Endpoint::parse(...), $seeds);
        $this->connectTimeout = $connectTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_CONNECT_TIMEOUT_SECS);
        $this->requestTimeout = $requestTimeout ?? TimeSpan::fromSeconds(self::DEFAULT_REQUEST_TIMEOUT_SECS);
        $this->clientSoftwareVersion = $clientSoftwareVersion ?? Package\version(self::PACKAGE_NAME);
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->seeds;
    }
}

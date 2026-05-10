<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime;

use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Protocol\VersionRange;

/**
 * @internal
 */
final class NegotiatedVersions implements \Countable
{
    /**
     * @param \SplObjectStorage<Generated\ApiKey, non-negative-int> $versions
     */
    public function __construct(
        private \SplObjectStorage $versions = new \SplObjectStorage(), // @phpstan-ignore parameter.defaultValue
    ) {}

    /**
     * @no-named-arguments
     */
    public function seed(Generated\ApiVersionsResponse\ApiVersion ...$versions): void
    {
        foreach ($versions as $entry) {
            $apiKey = Generated\ApiKey::tryFrom($entry->apiKey);
            if ($apiKey !== null) {
                $compatible = $apiKey
                    ->clientVersionRange()
                    ->negotiate(VersionRange::fromApiVersion($entry));

                if ($compatible !== null) {
                    $this->versions[$apiKey] = $compatible;
                }
            }
        }
    }

    /**
     * @return ?non-negative-int
     */
    public function versionOf(Generated\ApiKey $key): ?int
    {
        return $this->versions[$key] ?? null;
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->versions);
    }
}

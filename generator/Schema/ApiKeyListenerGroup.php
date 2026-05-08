<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Schema;

/**
 * @internal
 */
final class ApiKeyListenerGroup
{
    /** @var list<non-empty-string> */
    public private(set) array $listeners = [];

    /** @var list<ApiKey> */
    public private(set) array $keys = [];

    /**
     * @param list<non-empty-string> $listeners
     */
    public function __construct(
        array $listeners,
    ) {
        $this->listeners = $listeners;
    }

    public function addKey(ApiKey $key): void
    {
        $this->keys[] = $key;
    }
}

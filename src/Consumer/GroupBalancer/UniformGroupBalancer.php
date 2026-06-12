<?php

declare(strict_types=1);

namespace Thesis\Kafka\Consumer\GroupBalancer;

use Thesis\Kafka\Consumer\GroupBalancer;

/**
 * @api
 */
final readonly class UniformGroupBalancer implements GroupBalancer
{
    #[\Override]
    public function name(): string
    {
        return 'uniform';
    }
}

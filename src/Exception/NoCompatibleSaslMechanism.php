<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;

/**
 * @api
 */
final class NoCompatibleSaslMechanism extends Exception
{
    /**
     * @param non-empty-list<string> $configured
     * @param list<string> $supported
     */
    public function __construct(
        public readonly array $configured,
        public readonly array $supported = [],
    ) {
        parent::__construct(
            $supported === []
                ? \sprintf(
                    'None of the configured SASL mechanisms are supported: %s.',
                    implode(', ', $configured),
                )
                : \sprintf(
                    'None of the configured SASL mechanisms are supported. Configured: %s. Broker supports: %s.',
                    implode(', ', $configured),
                    implode(', ', $supported),
                ),
        );
    }
}

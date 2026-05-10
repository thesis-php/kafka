<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\KafkaException;

/**
 * @api
 */
final class AllSeedsFailed extends KafkaException
{
    /**
     * @param array<non-empty-string, \Throwable> $reasons endpoint → cause
     */
    public function __construct(
        public readonly array $reasons,
    ) {
        parent::__construct(self::formatMessage($reasons));
    }

    /**
     * @param array<non-empty-string, \Throwable> $reasons
     */
    private static function formatMessage(array $reasons): string
    {
        $message = \sprintf(
            'Unable to bootstrap from any of the %d configured seed endpoint(s):',
            \count($reasons),
        );

        foreach ($reasons as $endpoint => $reason) {
            $message .= PHP_EOL . PHP_EOL . $endpoint . ' -> ' . $reason::class;
            if ($reason->getMessage() !== '') {
                $message .= ': ' . $reason->getMessage();
            }
        }

        return $message;
    }
}

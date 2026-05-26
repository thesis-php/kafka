<?php

declare(strict_types=1);

namespace Thesis\Kafka\Exception;

use Thesis\Kafka\Exception;
use Thesis\Kafka\Protocol\Errors;

/**
 * @api
 */
final class ProtocolError extends Exception
{
    public function __construct(
        public readonly Errors $errors,
    ) {
        parent::__construct(self::formatMessage($errors));
    }

    private static function formatMessage(Errors $errors): string
    {
        $message = \sprintf(
            'Kafka request failed with %d protocol error(s):',
            \count($errors),
        );

        foreach ($errors as $error) {
            $message .= PHP_EOL . PHP_EOL . $error::class;
            $message .= " [{$error->code()->message()}]: {$error->message()}";
        }

        return $message;
    }
}

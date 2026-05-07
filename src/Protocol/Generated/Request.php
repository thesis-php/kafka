<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Encodable;

/**
 * @api
 * @generated
 * @template T of Response
 */
interface Request extends Encodable
{
    public function apiKey(): ApiKey;

    /**
     * @return class-string<T>
     */
    public function responseType(): string;
}

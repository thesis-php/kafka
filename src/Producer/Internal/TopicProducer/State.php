<?php

declare(strict_types=1);

namespace Thesis\Kafka\Producer\Internal\TopicProducer;

/**
 * @internal
 */
enum State
{
    case Ready;
    case Dead;
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Connection;

use Thesis\Endian;

/**
 * @internal
 */
final class ReadBuffer
{
    private string $buffer = '';

    /**
     * @return iterable<array-key, non-empty-string>
     */
    public function push(string $chunk): iterable
    {
        $this->buffer .= $chunk;

        while (\strlen($this->buffer) >= 4) {
            $length = Endian\Order::Network->unpackUint32(substr($this->buffer, 0, 4));
            if (\strlen($this->buffer) < 4 + $length) {
                break;
            }

            /** @var non-empty-string $frame */
            $frame = substr($this->buffer, 4, $length);
            $this->buffer = substr($this->buffer, 4 + $length);

            yield $frame;
        }
    }
}

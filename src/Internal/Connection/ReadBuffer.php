<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Connection;

use Thesis\Endian;
use Thesis\Kafka\Exception\UnexpectedValue;

/**
 * @internal
 */
final class ReadBuffer
{
    /**
     * The largest response frame that the client accepts: 1 GiB. A Fetch of
     * this client asks for 50 MiB by default. A broker caps a response with
     * fetch.max.bytes (55 MiB by default) and accepts a request of at most
     * socket.request.max.bytes (100 MiB by default), so a record batch is not
     * larger. A larger frame length is garbage, and the client must not buffer it.
     */
    private const int MAX_FRAME_BYTES = 1 << 30;

    private string $buffer = '';

    /**
     * @return iterable<array-key, non-empty-string>
     * @throws UnexpectedValue
     */
    public function push(string $chunk): iterable
    {
        $this->buffer .= $chunk;

        while (\strlen($this->buffer) >= 4) {
            $length = Endian\Order::Network->unpackUint32(substr($this->buffer, 0, 4));
            if ($length > self::MAX_FRAME_BYTES) {
                throw new UnexpectedValue(\sprintf(
                    'Response frame of %d bytes is larger than the limit of %d bytes.',
                    $length,
                    self::MAX_FRAME_BYTES,
                ));
            }

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

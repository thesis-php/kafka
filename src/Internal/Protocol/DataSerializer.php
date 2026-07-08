<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Protocol;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @internal
 */
final readonly class DataSerializer
{
    /**
     * @param non-negative-int $version
     */
    public function serialize(Encodable $encodable, int $version): string
    {
        $writer = new Writer();
        $writer->writeInt16($version);
        $encodable->encode($writer, $version);

        return (string) $writer;
    }

    /**
     * @template T of Decodable
     * @param class-string<T> $type
     * @return T
     */
    public function deserialize(string $type, string $bytes): object
    {
        $reader = new Reader($bytes);
        $version = $reader->readInt16();
        \assert($version >= 0);

        return $type::decode($reader, $version);
    }
}

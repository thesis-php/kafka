<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Protocol;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;

/**
 * @internal
 */
final readonly class Record
{
    /**
     * @param list<Record\Header> $headers
     */
    public function __construct(
        public int $timestampDelta,
        public int $offsetDelta,
        public ?string $key,
        public ?string $value,
        public array $headers = [],
        public int $attributes = 0,
    ) {}

    public function encode(Writer $writer): void
    {
        $body = new Writer();
        $body->writeInt8($this->attributes);
        $body->writeVarint($this->timestampDelta);
        $body->writeVarint($this->offsetDelta);
        $body->writeVarintBytes($this->key);
        $body->writeVarintBytes($this->value);
        $body->writeVarint(\count($this->headers));

        foreach ($this->headers as $header) {
            $header->encode($body);
        }

        $bodyBytes = (string) $body;

        $writer->writeVarint(\strlen($bodyBytes));
        $writer->write($bodyBytes);
    }

    public static function decode(Reader $reader): static
    {
        $length = $reader->readVarint();
        $body = new Reader($reader->readBytesLength($length));

        $attributes = $body->readInt8();
        $timestampDelta = $body->readVarint();
        $offsetDelta = $body->readVarint();
        $key = $body->readVarintBytes();
        $value = $body->readVarintBytes();
        $headersCount = $body->readVarint();
        $headers = [];

        for ($index = 0; $index < $headersCount; ++$index) {
            $headers[] = Record\Header::decode($body);
        }

        return new self(
            timestampDelta: $timestampDelta,
            offsetDelta: $offsetDelta,
            key: $key,
            value: $value,
            headers: $headers,
            attributes: $attributes,
        );
    }
}

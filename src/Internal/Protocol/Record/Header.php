<?php

declare(strict_types=1);

namespace Thesis\Kafka\Internal\Protocol\Record;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\Writer;
use Thesis\Kafka\RecordHeader;

/**
 * @internal
 */
final readonly class Header
{
    public function __construct(
        public string $key,
        public ?string $value,
    ) {}

    public static function fromRecordHeader(RecordHeader $header): self
    {
        return new self($header->key, $header->value);
    }

    public function encode(Writer $writer): void
    {
        $writer->writeVarintString($this->key);
        $writer->writeVarintBytes($this->value);
    }

    public static function decode(Reader $reader): static
    {
        return new self(
            key: $reader->readVarintString(),
            value: $reader->readVarintBytes(),
        );
    }
}

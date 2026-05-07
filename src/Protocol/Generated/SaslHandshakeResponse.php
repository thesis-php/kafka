<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;

/**
 * @api
 * @generated
 */
final readonly class SaslHandshakeResponse implements Response
{
    /**
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param list<string> $mechanisms The mechanisms enabled in the server.
     */
    public function __construct(
        public int $errorCode,
        public array $mechanisms,
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $errorCode = $reader->readInt16();
        $mechanismsCount = $reader->readArrayLength();
        $mechanisms = [];
        for ($mechanismsIndex = 0; $mechanismsIndex < $mechanismsCount; ++$mechanismsIndex) {
            $item = $reader->readString();
            $mechanisms[] = $item;
        }
        return new self($errorCode, $mechanisms);
    }
}

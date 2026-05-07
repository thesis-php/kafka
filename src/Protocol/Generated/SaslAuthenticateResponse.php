<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;

/**
 * @api
 * @generated
 */
final readonly class SaslAuthenticateResponse implements Response
{
    /**
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param string $authBytes The SASL authentication bytes from the server, as defined by the SASL mechanism.
     * @param int $sessionLifetimeMs Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public string $authBytes,
        public int $sessionLifetimeMs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $errorMessage = $reader->readCompactNullableString();
        } else {
            $errorMessage = $reader->readNullableString();
        }
        if ($flexible) {
            $authBytes = $reader->readCompactBytes();
        } else {
            $authBytes = $reader->readBytes();
        }
        $sessionLifetimeMs = 0;
        if ($version >= 1) {
            $sessionLifetimeMs = $reader->readInt64();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $authBytes, $sessionLifetimeMs, $unknownTags);
    }
}

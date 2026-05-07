<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterUserScramCredentialsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class AlterUserScramCredentialsResult implements Encodable, Decodable
{
    /**
     * @param string $user The user name.
     * @param int $errorCode The error code.
     * @param ?string $errorMessage The error message, if any.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $user,
        public int $errorCode,
        public ?string $errorMessage,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->user);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $user = $reader->readCompactString();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $unknownTags = $reader->readTaggedFields();
        return new self($user, $errorCode, $errorMessage, $unknownTags);
    }
}

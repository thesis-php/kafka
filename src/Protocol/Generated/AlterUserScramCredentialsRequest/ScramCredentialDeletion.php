<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\AlterUserScramCredentialsRequest;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class ScramCredentialDeletion implements Encodable, Decodable
{
    /**
     * @param string $name The user name.
     * @param int $mechanism The SCRAM mechanism.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public int $mechanism,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->name);
        $writer->writeInt8($this->mechanism);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $name = $reader->readCompactString();
        $mechanism = $reader->readInt8();
        $unknownTags = $reader->readTaggedFields();
        return new self($name, $mechanism, $unknownTags);
    }
}

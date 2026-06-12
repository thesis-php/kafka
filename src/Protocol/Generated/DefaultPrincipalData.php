<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DefaultPrincipalData implements Encodable, Decodable
{
    /**
     * @param string $type The principal type.
     * @param string $name The principal name.
     * @param bool $tokenAuthenticated Whether the principal was authenticated by a delegation token on the forwarding broker.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $type,
        public string $name,
        public bool $tokenAuthenticated,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->type);
        $writer->writeCompactString($this->name);
        $writer->writeBool($this->tokenAuthenticated);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $type = $reader->readCompactString();
        $name = $reader->readCompactString();
        $tokenAuthenticated = $reader->readBool();
        $unknownTags = $reader->readTaggedFields();
        return new self($type, $name, $tokenAuthenticated, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeUserScramCredentialsResponse\DescribeUserScramCredentialsResult;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CredentialInfo implements Encodable, Decodable
{
    /**
     * @param int $mechanism The SCRAM mechanism.
     * @param int $iterations The number of iterations used in the SCRAM credential.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $mechanism,
        public int $iterations,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt8($this->mechanism);
        $writer->writeInt32($this->iterations);
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $mechanism = $reader->readInt8();
        $iterations = $reader->readInt32();
        $unknownTags = $reader->readTaggedFields();
        return new self($mechanism, $iterations, $unknownTags);
    }
}

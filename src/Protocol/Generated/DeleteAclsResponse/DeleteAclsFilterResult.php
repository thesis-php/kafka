<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DeleteAclsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DeleteAclsFilterResult implements Encodable, Decodable
{
    /**
     * @param int $errorCode The error code, or 0 if the filter succeeded.
     * @param ?string $errorMessage The error message, or null if the filter succeeded.
     * @param list<DeleteAclsFilterResult\DeleteAclsMatchingAcl> $matchingAcls The ACLs which matched this filter.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public ?string $errorMessage,
        public array $matchingAcls,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeCompactNullableString($this->errorMessage);
        } else {
            $writer->writeNullableString($this->errorMessage);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->matchingAcls));
        } else {
            $writer->writeArrayLength(\count($this->matchingAcls));
        }
        foreach ($this->matchingAcls as $item) {
            $item->encode($writer, $version);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

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
            $matchingAclsCount = $reader->readCompactArrayLength();
        } else {
            $matchingAclsCount = $reader->readArrayLength();
        }
        $matchingAcls = [];
        for ($matchingAclsIndex = 0; $matchingAclsIndex < $matchingAclsCount; ++$matchingAclsIndex) {
            $item = DeleteAclsFilterResult\DeleteAclsMatchingAcl::decode($reader, $version);
            $matchingAcls[] = $item;
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $errorMessage, $matchingAcls, $unknownTags);
    }
}

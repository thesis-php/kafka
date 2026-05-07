<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeUserScramCredentialsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeUserScramCredentialsResult implements Encodable, Decodable
{
    /**
     * @param string $user The user name.
     * @param int $errorCode The user-level error code.
     * @param ?string $errorMessage The user-level error message, if any.
     * @param list<DescribeUserScramCredentialsResult\CredentialInfo> $credentialInfos The mechanism and related information associated with the user's SCRAM credentials.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $user,
        public int $errorCode,
        public ?string $errorMessage,
        public array $credentialInfos,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactString($this->user);
        $writer->writeInt16($this->errorCode);
        $writer->writeCompactNullableString($this->errorMessage);
        $writer->writeCompactArrayLength(\count($this->credentialInfos));
        foreach ($this->credentialInfos as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $user = $reader->readCompactString();
        $errorCode = $reader->readInt16();
        $errorMessage = $reader->readCompactNullableString();
        $credentialInfosCount = $reader->readCompactArrayLength();
        $credentialInfos = [];
        for ($credentialInfosIndex = 0; $credentialInfosIndex < $credentialInfosCount; ++$credentialInfosIndex) {
            $item = DescribeUserScramCredentialsResult\CredentialInfo::decode($reader, $version);
            $credentialInfos[] = $item;
        }
        $unknownTags = $reader->readTaggedFields();
        return new self($user, $errorCode, $errorMessage, $credentialInfos, $unknownTags);
    }
}

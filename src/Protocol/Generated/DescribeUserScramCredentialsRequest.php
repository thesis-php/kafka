<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeUserScramCredentialsResponse>
 */
final readonly class DescribeUserScramCredentialsRequest implements Request
{
    /**
     * @param ?list<DescribeUserScramCredentialsRequest\UserName> $users The users to describe, or null/empty to describe all users.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public ?array $users,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeUserScramCredentials;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeUserScramCredentialsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactNullableArrayLength($this->users === null ? null : \count($this->users));
        if ($this->users !== null) {
            foreach ($this->users as $item) {
                $item->encode($writer, $version);
            }
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

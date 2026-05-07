<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ApiVersionsResponse>
 */
final readonly class ApiVersionsRequest implements Request
{
    /**
     * @param string $clientSoftwareName The name of the client.
     * @param string $clientSoftwareVersion The version of the client.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $clientSoftwareName,
        public string $clientSoftwareVersion,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ApiVersions;
    }

    #[\Override]
    public function responseType(): string
    {
        return ApiVersionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 3;
        if ($version >= 3) {
            $writer->writeCompactString($this->clientSoftwareName);
        }
        if ($version >= 3) {
            $writer->writeCompactString($this->clientSoftwareVersion);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }
}

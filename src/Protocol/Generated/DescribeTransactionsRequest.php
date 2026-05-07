<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<DescribeTransactionsResponse>
 */
final readonly class DescribeTransactionsRequest implements Request
{
    /**
     * @param list<string> $transactionalIds Array of transactionalIds to include in describe results. If empty, then no results will be returned.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public array $transactionalIds,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::DescribeTransactions;
    }

    #[\Override]
    public function responseType(): string
    {
        return DescribeTransactionsResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeCompactArrayLength(\count($this->transactionalIds));
        foreach ($this->transactionalIds as $item) {
            $writer->writeCompactString($item);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

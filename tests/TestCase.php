<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Thesis\Kafka\Config\ProducerConfig;
use Thesis\Kafka\Config\SASLConfig;
use Thesis\Kafka\Protocol\Generated;

abstract readonly class TestCase
{
    /** @var non-empty-list<non-empty-string> */
    final public const array SEEDS = [
        'kafka-1:9092',
        'kafka-2:9092',
        'kafka-3:9092',
    ];
    final public const string CLUSTER_ID = '4L6g3nShT-eMCtK--X86sw';

    /**
     * @param non-empty-list<non-empty-string> $seeds
     */
    final protected function createClient(
        array $seeds = self::SEEDS,
        ?SASLConfig $sasl = null,
        ProducerConfig $producer = new ProducerConfig(),
    ): Client {
        return new Client(new Config(
            seeds: $seeds,
            sasl: $sasl,
            producer: $producer,
        ));
    }

    /**
     * @return non-empty-string
     */
    final protected function topicName(string $prefix = 'thesis-it'): string
    {
        return \sprintf('%s-%s', $prefix, bin2hex(random_bytes(6)));
    }

    final protected function metadataRequest(): Generated\MetadataRequest
    {
        return new Generated\MetadataRequest(
            topics: null,
            allowAutoTopicCreation: false,
            includeClusterAuthorizedOperations: false,
            includeTopicAuthorizedOperations: false,
        );
    }
}

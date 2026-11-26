<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Protocol\Generated\ApiKey;
use Thesis\Kafka\Testing\Kafka;

final readonly class ClientVersionsTest
{
    #[Test]
    public function clientDoesNotChooseBrokerOnlyOrUnstableVersions(): void
    {
        Kafka::connect(...Kafka::KAFKA_4_SEEDS)->test(static function (Kafka $kafka): void {
            Assert::same($kafka->negotiatedVersion(ApiKey::AddPartitionsToTxn), 3);
            Assert::int($kafka->negotiatedVersion(ApiKey::InitProducerId) ?? -1)->between(0, 5);
        });
    }
}

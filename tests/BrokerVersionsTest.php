<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Data\DataProvider;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\Setup;

final readonly class BrokerVersionsTest
{
    #[Test]
    #[DataProvider([Kafka::class, 'versions'])]
    public function producesAndConsumesOneRecord(Setup $setup): void
    {
        $setup->test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer();

            $produced = $producer->produceSync(new Producer\Record(
                value: 'one-record',
                topic: $topic,
                key: 'key-1',
            ));

            Assert::same($produced->topic, $topic);
            Assert::same($produced->partition, 0);
            Assert::same($produced->offset, 0);

            $consumer = $kafka->directConsumer($topic, new Consumer\Config(
                autoOffsetReset: AutoOffsetReset::Earliest,
            ));

            $records = $kafka->consume($consumer, count: 1);
            \assert(\count($records) === 1);

            Assert::same($records[0]->topic, $topic);
            Assert::same($records[0]->partition, 0);
            Assert::same($records[0]->offset, 0);
            Assert::same($records[0]->key, 'key-1');
            Assert::same($records[0]->value, 'one-record');
        });
    }
}

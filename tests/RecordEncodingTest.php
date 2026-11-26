<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Testing\Kafka;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class RecordEncodingTest
{
    private const int LARGE_FETCH_RECORDS = 40_000;

    /**
     * The limit of the topic and of a producer batch. It holds all records of
     * the large fetch in one batch.
     */
    private const int LARGE_BATCH_BYTES = 16 << 20;

    /**
     * The time limit to read all records of the large fetch. A linear decoder
     * reads them in about 0.15 s. The old varint decoder needs about 28 s.
     */
    private const float LARGE_FETCH_SECONDS = 3.0;

    /**
     * Lengths where the varint of a length gets one more byte: 63 and 8,191
     * for a zigzag varint, 127 and 16,383 for an unsigned varint.
     */
    private const array LENGTHS = [0, 63, 64, 127, 128, 8_191, 8_192, 16_383, 16_384];

    /**
     * Timestamps near the limits of 32 and 64 bits. In one batch they give
     * timestamp deltas from 1 to 10 bytes, positive and negative.
     */
    private const array TIMESTAMPS = [
        0,
        2_147_483_647,
        2_147_483_648,
        4_294_967_295,
        4_294_967_296,
        PHP_INT_MAX,
        PHP_INT_MAX - 1,
        1,
    ];
    private const int HEADERS = 300;

    #[Test]
    public function edgeValuesSurviveProduceAndConsume(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic();
            $producer = $kafka->producer(new Producer\Config(batchSize: self::LARGE_BATCH_BYTES));

            /** @var list<array{?string, ?string, list<RecordHeader>, non-negative-int}> $expected key, value, headers, timestamp */
            $expected = [];

            foreach (self::LENGTHS as $length) {
                $expected[] = [str_repeat('k', $length), $length === 0 ? '' : random_bytes($length), [], 1_000];
            }

            $expected[] = [null, null, [], 1_000];

            $headers = [new RecordHeader('', null), new RecordHeader('empty', '')];
            for ($i = 2; $i < self::HEADERS; ++$i) {
                $headers[] = new RecordHeader("header-{$i}", str_repeat('h', $i));
            }

            $expected[] = ['headers', 'many', $headers, 1_000];

            foreach (self::TIMESTAMPS as $timestamp) {
                $expected[] = ['timestamp', (string) $timestamp, [], $timestamp];
            }

            $produced = $producer->produceSync(array_map(
                static fn(array $record) => new Producer\Record(
                    value: $record[1],
                    key: $record[0],
                    topic: $topic,
                    headers: $record[2],
                    partition: 0,
                    timestamp: $record[3],
                ),
                $expected,
            ));

            Assert::same(array_map(static fn(Producer\Produced $p) => $p->offset, $produced), array_keys($expected));

            $records = $kafka->consume($kafka->directConsumer($topic), count: \count($expected));

            // Headers are objects, so they are compared by value.
            Assert::equals(
                array_map(static fn(Consumer\Record $r) => [$r->key, $r->value, $r->headers, $r->timestamp], $records),
                $expected,
            );
            Assert::same(
                array_map(static fn(Consumer\Record $r) => [$r->key, $r->value, $r->timestamp], $records),
                array_map(static fn(array $record) => [$record[0], $record[1], $record[3]], $expected),
            );
            Assert::same(array_map(static fn(Consumer\Record $r) => $r->offset, $records), array_keys($expected));
        });
    }

    #[Test]
    public function readsLargeBatchInLinearTime(): void
    {
        Kafka::test(static function (Kafka $kafka): void {
            $topic = $kafka->topic(configs: ['max.message.bytes' => (string) self::LARGE_BATCH_BYTES]);
            $producer = $kafka->producer(new Producer\Config(batchSize: self::LARGE_BATCH_BYTES));

            $records = array_map(
                static fn(int $i) => new Producer\Record(
                    value: self::largeValue($i),
                    key: "key-{$i}",
                    topic: $topic,
                    headers: [new RecordHeader('h', 'x')],
                    partition: 0,
                ),
                range(0, self::LARGE_FETCH_RECORDS - 1),
            );

            $produced = $producer->produceSync($records);
            Assert::same($produced[self::LARGE_FETCH_RECORDS - 1]->offset ?? null, self::LARGE_FETCH_RECORDS - 1);

            $consumer = $kafka->directConsumer($topic);

            $start = hrtime(true);
            $read = $kafka->consume($consumer, count: self::LARGE_FETCH_RECORDS);
            $elapsed = (hrtime(true) - $start) / 1e9;

            Assert::count($read, self::LARGE_FETCH_RECORDS);
            Assert::float($elapsed)->lessThan(self::LARGE_FETCH_SECONDS);

            foreach ([0, 1, 20_000, self::LARGE_FETCH_RECORDS - 1] as $offset) {
                Assert::same($read[$offset]->offset ?? null, $offset);
                Assert::same($read[$offset]->key ?? null, "key-{$offset}");
                Assert::same($read[$offset]->value ?? null, self::largeValue($offset));
            }
        });
    }

    private static function largeValue(int $i): string
    {
        return \sprintf('value-%05d-%s', $i, str_repeat('v', 90));
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Amp\Future;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Retry;
use Testo\Test;
use Thesis\Kafka\Consumer\AutoOffsetReset;
use Thesis\Kafka\Consumer\ConsumeMode;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

#[Retry(maxAttempts: 5, markFlaky: true)]
final readonly class GroupConsumerTest extends TestCase
{
    #[Test]
    public function oneConsumerReadsEveryPartition(): void
    {
        $client = $this->createClient();

        $topic = $this->createTopic($client, prefix: 'thesis-group', partitions: 2);
        $this->waitUntilTopicVisible($client, $topic, partitions: 2);

        $producer = $client->createProducer();

        $producer->produceSync([
            new Producer\Record(value: 'p0-0', topic: $topic, partition: 0),
            new Producer\Record(value: 'p0-1', topic: $topic, partition: 0),
            new Producer\Record(value: 'p1-0', topic: $topic, partition: 1),
            new Producer\Record(value: 'p1-1', topic: $topic, partition: 1),
        ]);

        $consumer = self::createGroupConsumer($client, $topic, self::generateGroupId());

        $records = self::consumeUntil($consumer, 4);

        Assert::count($records, 4);
        Assert::same(
            self::sort($records, static fn(Consumer\Record $record) => $record->value),
            ['p0-0', 'p0-1', 'p1-0', 'p1-1'],
        );

        Assert::same(
            self::sort($records, static fn(Consumer\Record $record) => $record->partition),
            [0, 1],
        );

        $consumer->close();
        $producer->close();

        $this->deleteTopic($client, $topic);

        $client->close();
    }

    #[Test]
    public function readAfterCommit(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-group');
        $this->waitUntilTopicVisible($client, $topic);

        $producer = $client->createProducer();

        $producer->produceSync([
            new Producer\Record(value: 'r0', topic: $topic, partition: 0),
            new Producer\Record(value: 'r1', topic: $topic, partition: 0),
            new Producer\Record(value: 'r2', topic: $topic, partition: 0),
            new Producer\Record(value: 'r3', topic: $topic, partition: 0),
        ]);

        $groupId = self::generateGroupId();

        $first = self::createGroupConsumer($client, $topic, $groupId);
        $consumed = self::consumeUntil($first, 2);

        $processed = \array_slice($consumed, 0, 2);
        \assert(\count($processed) === 2);

        Assert::same($processed[0]->value, 'r0');
        Assert::same($processed[1]->value, 'r1');

        $first->commitRecords($processed);
        $first->close();

        $second = self::createGroupConsumer($client, $topic, $groupId);
        $resumed = self::consumeUntil($second, 2);

        $prefix = \array_slice($resumed, 0, 2);
        \assert(\count($prefix) === 2);

        Assert::same($prefix[0]->value, 'r2');
        Assert::same($prefix[1]->value, 'r3');
        Assert::same($prefix[0]->offset, 2);
        Assert::same($prefix[1]->offset, 3);

        $values = array_map(static fn(Consumer\Record $record) => $record->value, $resumed);
        Assert::false(\in_array('r0', $values, true));
        Assert::false(\in_array('r1', $values, true));

        $second->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function commitUncommitted(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-group');
        $this->waitUntilTopicVisible($client, $topic);

        $producer = $client->createProducer();
        $producer->produceSync([
            new Producer\Record(value: 'r0', topic: $topic, partition: 0),
            new Producer\Record(value: 'r1', topic: $topic, partition: 0),
        ]);

        $groupId = self::generateGroupId();

        $first = self::createGroupConsumer($client, $topic, $groupId);
        $consumed = self::consumeUntil($first, 2);

        $processed = \array_slice($consumed, 0, 2);
        \assert(\count($processed) === 2);

        Assert::same($processed[0]->value, 'r0');
        Assert::same($processed[1]->value, 'r1');

        $first->commitUncommitted();
        $first->close();

        $producer->produceSync([
            new Producer\Record(value: 'r2', topic: $topic, partition: 0),
            new Producer\Record(value: 'r3', topic: $topic, partition: 0),
        ]);

        $second = self::createGroupConsumer($client, $topic, $groupId);
        $resumed = self::consumeUntil($second, 2);

        $prefix = \array_slice($resumed, 0, 2);
        \assert(\count($prefix) === 2);

        Assert::same($prefix[0]->value, 'r2');
        Assert::same($prefix[1]->value, 'r3');
        Assert::same($prefix[0]->offset, 2);
        Assert::same($prefix[1]->offset, 3);

        $values = array_map(static fn(Consumer\Record $record) => $record->value, $resumed);
        Assert::false(\in_array('r0', $values, true));
        Assert::false(\in_array('r1', $values, true));

        $second->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function autocommit(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-group');
        $this->waitUntilTopicVisible($client, $topic);

        $producer = $client->createProducer();
        $producer->produceSync([
            new Producer\Record(value: 'r0', topic: $topic, partition: 0),
            new Producer\Record(value: 'r1', topic: $topic, partition: 0),
        ]);

        $groupId = self::generateGroupId();

        $first = self::createGroupConsumer(
            $client,
            $topic,
            $groupId,
            autocommit: true,
            autocommitInterval: TimeSpan::fromSeconds(1),
        );

        $consumed = self::consumeUntil($first, 2);
        \assert(\count($consumed) === 2);

        Assert::same($consumed[0]->value, 'r0');
        Assert::same($consumed[1]->value, 'r1');

        delay(1.5);
        $first->close();

        $producer->produceSync([
            new Producer\Record(value: 'r2', topic: $topic, partition: 0),
            new Producer\Record(value: 'r3', topic: $topic, partition: 0),
        ]);

        $second = self::createGroupConsumer($client, $topic, $groupId);
        $resumed = self::consumeUntil($second, 2);

        $prefix = \array_slice($resumed, 0, 2);
        \assert(\count($prefix) === 2);

        Assert::same($prefix[0]->value, 'r2');
        Assert::same($prefix[1]->value, 'r3');
        Assert::same($prefix[0]->offset, 2);
        Assert::same($prefix[1]->offset, 3);

        $values = array_map(static fn(Consumer\Record $record) => $record->value, $resumed);
        Assert::false(\in_array('r0', $values, true));
        Assert::false(\in_array('r1', $values, true));

        $second->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    #[Test]
    public function twoMembersEachOwnOnePartition(): void
    {
        $client = $this->createClient();
        $topic = $this->createTopic($client, prefix: 'thesis-group', partitions: 2);
        $this->waitUntilTopicVisible($client, $topic, partitions: 2);

        $groupId = self::generateGroupId();

        $first = self::createGroupConsumer($client, $topic, $groupId);
        $second = self::createGroupConsumer($client, $topic, $groupId);

        /** @var Future<list<Consumer\Record>> $firstFuture */
        $firstFuture = async(self::consumeUntil(...), $first, 2);

        /** @var Future<list<Consumer\Record>> $secondFuture */
        $secondFuture = async(self::consumeUntil(...), $second, 2);

        $producer = $client->createProducer();
        $producer->produceSync([
            new Producer\Record(value: 'p0-0', topic: $topic, partition: 0),
            new Producer\Record(value: 'p0-1', topic: $topic, partition: 0),
            new Producer\Record(value: 'p1-0', topic: $topic, partition: 1),
            new Producer\Record(value: 'p1-1', topic: $topic, partition: 1),
        ]);

        [$recordsA, $recordsB] = Future\await([$firstFuture, $secondFuture]); // @phpstan-ignore offsetAccess.notFound, offsetAccess.notFound

        $partitionsA = self::sort($recordsA, static fn(Consumer\Record $record) => $record->partition);
        \assert(\count($partitionsA) === 1);

        $partitionsB = self::sort($recordsB, static fn(Consumer\Record $record) => $record->partition);
        \assert(\count($partitionsB) === 1);

        // Each member owns exactly one, distinct partition, and together they cover
        // every produced record.
        Assert::count($partitionsA, 1);
        Assert::count($partitionsB, 1);
        Assert::notSame($partitionsA[0], $partitionsB[0]);
        Assert::same(
            self::sort(
                [...$recordsA, ...$recordsB],
                static fn(Consumer\Record $record) => $record->value,
            ),
            ['p0-0', 'p0-1', 'p1-0', 'p1-1'],
        );

        $first->close();
        $second->close();
        $producer->close();
        $this->deleteTopic($client, $topic);
        $client->close();
    }

    /**
     * @param non-empty-string $groupId
     * @param non-empty-string $topic
     */
    private static function createGroupConsumer(
        Client $client,
        string $topic,
        string $groupId,
        bool $autocommit = false,
        ?TimeSpan $autocommitInterval = null,
    ): Consumer {
        return $client->createConsumer($topic, new Consumer\Config(
            autoOffsetReset: AutoOffsetReset::Earliest,
            group: new Consumer\GroupConfig(
                groupId: $groupId,
                sessionTimeout: TimeSpan::fromSeconds(6),
                heartbeatInterval: TimeSpan::fromSeconds(1),
                autocommitInterval: $autocommitInterval,
                rebalanceTimeout: TimeSpan::fromSeconds(6),
                autocommit: $autocommit,
            ),
        ));
    }

    /**
     * @param positive-int $expected
     * @return list<Consumer\Record>
     */
    private static function consumeUntil(Consumer $consumer, int $expected): array
    {
        $records = [];

        while (\count($records) < $expected) {
            foreach ($consumer->consume(ConsumeMode::Poll, new TimeoutCancellation(0.5)) as $batch) {
                $records = [
                    ...$records,
                    ...$batch,
                ];
            }
        }

        return $records;
    }

    /**
     * @return non-empty-string
     */
    private static function generateGroupId(): string
    {
        return \sprintf('thesis-group-%s', bin2hex(random_bytes(6)));
    }

    /**
     * @template T of int|string|null
     * @param list<Consumer\Record> $records
     * @param callable(Consumer\Record): T $map
     * @return list<T>
     */
    private static function sort(array $records, callable $map): array
    {
        $values = array_values(array_unique(array_map(
            $map,
            $records,
        )));

        sort($values);

        return $values;
    }
}

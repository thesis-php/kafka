# Thesis Kafka

A pure async PHP client for Apache Kafka.

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Producer](#producer)
  - [Produce](#produce)
  - [Sync produce](#sync-produce)
  - [Idempotent producer](#idempotent-producer)
  - [Manual flush](#manual-flush)
  - [Timeouts and retries](#timeouts-and-retries)
  - [Close](#close)
- [Consumer](#consumer)
  - [Manual consume](#manual-consume)
  - [Group consumer](#group-consumer)
  - [Start position](#start-position)
  - [Poll loop](#poll-loop)
  - [Close and errors](#close-and-errors)
  - [Commit](#commit)
  - [Autocommit](#autocommit)
  - [Callback API](#callback-api)
  - [Fetch errors](#fetch-errors)
- [Logging](#logging)
- [Examples](#examples)

## Installation

```shell
composer require thesis/kafka
```

## Quick start

```php
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;

$client = new Client(new Config(
    seeds: ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
));
```

The examples below skip the bootstrap (autoload, `Client` setup). See the
[examples/](examples) directory for full runnable files.

## Producer

Create a producer from the client. `defaultTopic` is used when a record has no
topic of its own.

```php
use Thesis\Kafka\Producer;

$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
));
```

### Produce

`produce()` buffers the record and returns a `Future`. Await it to get the
result. You do not have to await right away: send many records, await later.

```php
$future = $producer->produce(new Producer\Record('hello'));

$produced = $future->await();
```

### Sync produce

`produceSync()` sends the records and waits for the broker acknowledgement. It
does not block the event loop: other coroutines keep running while it waits.
Pass one record or a list.

A cancellation ends the wait with `Amp\CancelledException`. The records that
were queued are still sent.

```php
$producer->produceSync([
    new Producer\Record(value: 'a', topic: 'events'),
    new Producer\Record(value: 'b', topic: 'events'),
]);
```

### Idempotent producer

Set `idempotent: true`. The broker then drops duplicates, so a retry does not
write the same record twice.

```php
$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
    idempotent: true,
));
```

A record can still fail, for example when the broker rejects it or when all
attempts run out of time. Only the futures of this record and of the records
in its batch fail. The producer
then raises its epoch, and the partition starts its sequence at 0 again. So
the next records of the partition are delivered, in the order of `produce()`.
After a timeout the producer does not know whether the broker wrote the failed
record. It may be in the topic once.

### Manual flush

With `manualFlush: true` the producer does not send batches on its own. You
call `flush()` when you want them sent. Useful for grouping many records into
few requests.

```php
$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
    manualFlush: true,
));

$futures = [];
for ($i = 0; $i < 100; ++$i) {
    $futures[] = $producer->produce(new Producer\Record("record={$i}"));
}

$producer->flush();
```

See [examples/produce-manual-flush.php](examples/produce-manual-flush.php).

### Timeouts and retries

Each attempt to send a batch has a time limit: `produceTimeout` of the producer
plus `requestTimeout` of the client. The broker can use the whole
`produceTimeout` to wait for the replicas. The rest is for the network.

When a broker does not answer in this time, the attempt fails like a lost
connection. A connection that was silent for the whole time is closed, and the
next attempt uses a new one. The producer makes at most `produceAttempts`
attempts. When the last one gets no answer, the futures of the records fail
with `Exception\ProduceTimedOut`.

Without `idempotent: true`, a retry can write the batch twice: the broker can
get the first attempt and only the answer is lost.

```php
use Thesis\Kafka\Exception\ProduceTimedOut;
use Thesis\Time\TimeSpan;

$producer = $client->createProducer(new Producer\Config(
    defaultTopic: 'events',
    produceTimeout: TimeSpan::fromSeconds(10),
    produceAttempts: 3,
));

try {
    $producer->produceSync(new Producer\Record('hello'));
} catch (ProduceTimedOut $e) {
    echo "No answer for {$e->topic}-{$e->partition}\n";
}
```

See [examples/produce-timeout.php](examples/produce-timeout.php).

A slow partition makes only the callers of this partition wait. When a
`produce()` fills a batch, the next calls for this partition wait until the
partition takes this batch to send it. The calls for other partitions and
topics do not wait.

### Close

`close()` sends the open batches at once, without the linger time. Then it
waits until every record has a result, and closes the connections. After the
call, `produce()` throws `Exception\ProducerClosed`.

```php
$producer->close();
```

Each attempt has a time limit, but with retries this wait can be long. Give a
cancellation to limit it. When it fires, the records that are not delivered
yet fail with `Amp\CancelledException`, and the producer is closed anyway.
`close()` does not throw in this case. Check the futures of the records.

```php
use Amp\TimeoutCancellation;

$producer->close(new TimeoutCancellation(5));
```

Use `flush()` when you want to wait for the records but keep the producer
open. See [examples/produce-close.php](examples/produce-close.php).

## Consumer

### Manual consume

Without a group, the consumer reads every partition of the topic. Iterate the
batches it yields.

```php
$consumer = $client->createConsumer(['events']);

foreach ($consumer->consume() as $batch) {
    // handle $batch
}
```

See [examples/consume-manual.php](examples/consume-manual.php).

### Group consumer

Pass a `GroupConfig` to join a consumer group. Partitions are shared across all
members of the group and rebalanced when members come and go.

```php
use Thesis\Kafka\Consumer;

$consumer = $client->createConsumer(['events'], new Consumer\Config(
    group: new Consumer\GroupConfig(
        groupId: 'thesis-consumer',
    ),
));

foreach ($consumer->consume() as $batch) {
    // handle $batch
    $consumer->commitRecords($batch);
}
```

See [examples/consume-group.php](examples/consume-group.php).

### Start position

`autoOffsetReset` tells the consumer where to read when it has no position.
This happens in two cases:

- a group has no committed offset for a partition, or a consumer has no group;
- the position is out of the range of the log. For example, the retention of
  the topic has deleted the records at the position.

| Value | What the consumer does |
|-------|------------------------|
| `AutoOffsetReset::Earliest` (default) | Reads from the first record in the log. |
| `AutoOffsetReset::Latest` | Reads only the records that come after the reset. |
| `AutoOffsetReset::None` | Does not choose. A group consumer without a committed offset throws `Exception\NoOffsetForPartitions`. A position out of range throws `Exception\OffsetOutOfRange`. |

A consumer without a group has no committed offset, so it cannot use `None`:
`createConsumer()` and `consume()` throw `Exception\InvalidConfiguration`.

```php
use Thesis\Kafka\Consumer;
use Thesis\Kafka\Exception\OffsetOutOfRange;

$consumer = $client->createConsumer(['events'], new Consumer\Config(
    autoOffsetReset: Consumer\AutoOffsetReset::None,
    group: new Consumer\GroupConfig(groupId: 'thesis-consumer'),
));

try {
    foreach ($consumer->consume() as $batch) {
        // handle $batch
    }
} catch (OffsetOutOfRange $e) {
    echo "No records at offset {$e->offset} of {$e->partition}\n";
}
```

### Poll loop

`ConsumeMode::Poll` waits for the first batch. Then it also returns the batches
that are in the buffer of the consumer at this moment, without a wait, and ends.
When the timeout fires before the first batch, it returns nothing. Without a
cancellation, `pollTimeout` of the consumer config is the timeout. Use it when
you want to do other work between polls.

`prefetchBufferSize` of the consumer config limits the buffer. When the buffer
is full, the fetch waits for the reader.

```php
use Amp\TimeoutCancellation;
use Thesis\Kafka\Consumer;

while (true) {
    foreach ($consumer->consume(Consumer\ConsumeMode::Poll, new TimeoutCancellation(1.0)) as $batch) {
        // handle $batch
    }
}
```

### Close and errors

Only one `consume()` reads at a time. A second call throws
`Exception\ConsumerBusy`. The iterable of `consume()` holds the consumer until
its iteration ends or the iterable is destroyed.

When the cancellation of `consume()` fires, the iterable gives no other batch
and ends without an exception. The batches in the buffer stay for the next
`consume()`, so no record is lost.

`close()` stops the fetch, leaves the group and closes the connections. A
running `consume()` gives no other batch and ends without an exception. The
batches in the buffer are dropped. `close()` can be called many times, also
before the first `consume()`. A closed consumer throws
`Exception\ConsumerClosed` from every `consume()`.

A fatal error closes the consumer at once: the fetch stops, autocommit stops, a
group member leaves the group, and the connections are closed. Fatal errors are
the errors of a fetch that no retry can fix (see [Fetch errors](#fetch-errors))
and every failure of the group: join, sync, heartbeat, offsets. A request of the
group that gets no answer in time, also on the last attempt, is a fatal error
too: `Exception\RequestTimedOut`. The waiting `consume()` throws the error. When
no `consume()` waits, the next one throws it. After that `consume()` throws
`Exception\ConsumerClosed`. An exception of your own code inside the `foreach`
does not close the consumer.

A failed start of a consumer without a group is not fatal. For example, the
broker is not reachable. The next `consume()` tries to start again.

```php
use Thesis\Kafka\Exception\ConsumerClosed;

try {
    foreach ($consumer->consume() as $batch) {
        // handle $batch
    }
} catch (ConsumerClosed) {
    // the consumer was closed before
} catch (\Throwable $e) {
    // a fatal error: the consumer is closed now
}
```

### Commit

There are two ways to commit in a group.

`commitRecords()` commits the records you pass. For each partition it commits the
highest offset.

```php
$consumer->commitRecords($batch);
```

`commitUncommitted()` commits the position the consumer tracked for you, across
all owned partitions. You do not pass the records back. Already committed
offsets are skipped, so it is a no-op when nothing new arrived.

```php
$consumer->commitUncommitted();
```

See [examples/commit-uncommitted.php](examples/commit-uncommitted.php).

### Autocommit

Set `autocommit: true` to commit the delivered position on a timer. Tune the
period with `autocommitInterval`.

```php
use Thesis\Time\TimeSpan;
use Thesis\Kafka\Consumer;

$consumer = $client->createConsumer(['events'], new Consumer\Config(
    group: new Consumer\GroupConfig(
        groupId: 'thesis-consumer',
        autocommit: true,
        autocommitInterval: TimeSpan::fromSeconds(5),
    ),
));
```

### Callback API

`Client::consume()` runs a callback for each batch in the background. It returns
a context you close and join on shutdown. The loop runs until it is closed or
fails, also when you do not keep the context. The callback can close the
context too: the loop then gives it no other batch.

```php
use Thesis\Kafka\Consumer;
use Thesis\Kafka\ConsumerSession;
use function Amp\trapSignal;

$ctx = $client->consume(['events'], static function (
    array $records,
    ConsumerSession $session,
): void {
    // handle $records
    $session->commitRecords($records);
}, new Consumer\Config(group: new Consumer\GroupConfig(
    groupId: 'thesis-consumer',
)));

trapSignal([\SIGINT, \SIGTERM]);
$ctx->close();
$ctx->join();
```

See [examples/consume-group-callable.php](examples/consume-group-callable.php).

### Fetch errors

The consumer reacts to each error of a fetch in one of three ways.

- **Offset out of range.** The consumer resets the position by
  `autoOffsetReset` (see [Start position](#start-position)) and reads on. With
  `None` it stops.
- **An error that a new attempt can fix.** For example, the leader of a
  partition has moved, a broker is down or does not answer. The consumer waits
  a little, asks for new metadata and tries again, without a limit of attempts.
  The pause grows up to 1 s. A partition goes on from its position at its
  new leader, so no record is lost or read twice. The other partitions do not
  wait for it. To stop the wait, cancel `consume()` or close the consumer.
- **Any other error.** For example, a partition error that no retry can fix,
  or a batch that the consumer cannot decode (`Exception\UnsupportedCodec`,
  `Exception\CorruptRecordBatch`). The consumer stops reading all partitions,
  `consume()` throws the error, and the consumer is closed (see
  [Close and errors](#close-and-errors)). A partition error comes as
  `Exception\ProtocolError`. A group consumer leaves the group. With
  `Client::consume()`, `join()` throws the error.

Each fetch has a time limit: `maxWait` of the consumer plus `requestTimeout` of
the client. A connection that was silent for this whole time is closed, and the
next attempt uses a new one.

## Logging

`Client` accepts any PSR-3 logger as its second argument.

```php
use Thesis\Kafka\Client;
use Thesis\Kafka\Config;

$client = new Client(new Config(
    seeds: ['kafka-1:9092'],
), $logger);
```

See [examples/logger.php](examples/logger.php).

## Examples

Runnable examples live in the [examples](examples) directory.
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
  - [Poll loop](#poll-loop)
  - [Commit](#commit)
  - [Autocommit](#autocommit)
  - [Callback API](#callback-api)
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

### Poll loop

`ConsumeMode::Poll` returns one batch, or nothing when the timeout fires. Use it
when you want to do other work between polls.

```php
use Amp\TimeoutCancellation;
use Thesis\Kafka\Consumer;

while (true) {
    foreach ($consumer->consume(Consumer\ConsumeMode::Poll, new TimeoutCancellation(1.0)) as $batch) {
        // handle $batch
    }
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
a context you close and join on shutdown.

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
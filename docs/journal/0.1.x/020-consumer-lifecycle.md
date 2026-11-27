# 020 Consumer lifecycle

Type: fixes

## Goal

A consumer has a clear state at every moment: not started, reading, or
closed. A failed, cancelled or dropped `consume()` does not break the next
one. `close()` and a fatal error stop the delivery at once and release
everything the consumer holds. Today several paths leave a consumer busy
forever, running without an owner, or closed without a word.

## Context

- The findings are in `audit-findings.md`, section "020". Read it first.
  They were checked against the code when this task was planned. The note
  "Planned in task 020" at the end of the section says what is still true.
- Both kinds of consumer implement `Thesis\Kafka\Consumer`
  (`src/Consumer.php`): `Consumer\Internal\DirectConsumer` and
  `Consumer\Internal\GroupConsumer`. Both read through
  `Consumer\Internal\AssignmentConsumer`. `Client::consume()` wraps a
  consumer into `Consumer\Internal\CallableHandler`, which implements
  `ConsumerSubscription`.
- The busy flag. `AssignmentConsumer::consume()` and
  `GroupConsumer::consume()` set their `ConsumeState` to `Running` and
  return a generator (`iterate()`, `createIterator()`). Only the `finally`
  of the generator sets the state back. The body of a generator that was
  never iterated does not run, so its `finally` does not run either. An
  iterable that is dropped without iteration leaves the consumer busy
  forever. `DirectConsumerTest::concurrentConsumeThrowsConsumerBusy` calls
  `consume()` two times and iterates nothing. It expects `ConsumerBusy`
  from the second call.
- The cancellation. Both generators wait with
  `ConcurrentIterator::continue($cancellation)`. `continue()` of
  `amphp/pipeline` returns a value that is ready and does not look at the
  cancellation. So a `consume()` with a time limit does not end while
  records keep coming. Task 021 fixed the same thing in
  `GroupConsumer\GroupSession::run()` with a check before each push.
- `close()` of a direct consumer. `AssignmentConsumer::stop()` completes
  the queue. A completed queue still gives its buffered values, so a
  running `consume()` yields batches after `close()`. The group consumer
  drops such batches already (`Delivery::$session->live()`, task 021).
- Poll mode. The doc of `Consumer\ConsumeMode::Poll` says: wait for the
  first batch, then return what is in the buffer too.
  `AssignmentConsumer::iterate()` and `GroupConsumer::createIterator()`
  yield one batch in Poll mode. `AssignmentConsumer::start()` creates its
  queue without a size, so a direct consumer does not use
  `Consumer\Config::$prefetchBufferSize`. The group consumer gives it to
  its own queue.
- A fatal error. Since task 019 an error of the fetch that cannot be fixed
  goes to the queue and leaves `consume()`. An errored queue of
  `amphp/pipeline` throws the same error from every later `continue()`.
  `GroupConsumer::run()` puts every other failure of the group loop into
  its queue the same way. After that the state is still `Running`, the
  autocommit timer (`GroupConsumer::$autocommitId`) still fires, and the
  connections of the `Cluster` stay open until `close()`. The member has
  left the group already.
- A silent end. `GroupConsumer::run()` catches every `CancelledException`
  and completes the queue. `GroupConsumer\Coordinator::request()` throws
  the `CancelledException` of its own time limit after its last attempt.
  Then `consume()` returns as after a `close()`, and the user sees no
  error. `createIterator()` and `AssignmentConsumer::iterate()` also catch
  `CancelledException` and return, so this exception cannot carry an error
  to the user.
- `GroupConsumer::close()` returns at once when the state is not
  `Running`. A `close()` before the first `consume()` leaves the state
  `Idle`, and a later `consume()` starts the consumer.
- The setup of a direct consumer. `DirectConsumer::setup()` sets the state
  to `Idle` when the setup fails. It does not check the state first, so a
  `close()` during the setup is lost: `Closed` becomes `Idle`.
  `AssignmentConsumer::start()` sets its state to `Running` before the
  work that can throw. After such a failure the next `start()` throws
  `\LogicException('Consumer already running.')`. Task 039 fixed the
  cancelled first `consume()`: the setup is shared and does not use the
  cancellation of a caller.
- The subscription. `CallableHandler::close()` cancels the loop and calls
  `join()`. From inside the handler this waits for the coroutine that
  calls it, so it never returns. `CallableHandler::__destruct()` cancels
  the loop, so a subscription whose handle is not stored stops at once.
  The doc of `Client::consume()` says that the loop runs until `close()`.
- The test kit has `loopWorkAfter($step)`. It returns the event loop work
  that a step left behind, for example a timer.

## What to do

Give the consumer three clear states and make every path keep them.

A consumer is held by a `consume()` from the call until its iterable ends
or is destroyed. Nothing else can leave it busy.

The cancellation of a `consume()` and `close()` stop the delivery at once.
No batch is yielded after either of them, also when the buffer has
batches.

A fatal error closes the consumer. The error leaves the `consume()` that
is waiting. Then the consumer is in the same state as after `close()`:
nothing runs in the background and nothing is held.

A time limit of the group loop that nobody asked for is such a fatal
error, with its own exception. It is not a normal end.

Poll mode works as its doc says, for both kinds of consumer.

A subscription runs until it is closed or fails, and it can be closed
from inside its handler.

## Decisions

- States. A consumer is closed after `close()` and after a fatal error. A
  closed consumer throws `Exception\ConsumerClosed` from every later
  `consume()`. `close()` is idempotent in every state.
- `close()` before the first `consume()` closes the consumer too, for both
  kinds. A later `consume()` throws `ConsumerClosed`. It sends no request.
- A fatal error is every exception that leaves `consume()` after the
  consumer has started: an error of the fetch that task 019 throws
  (`OffsetOutOfRange`, `ProtocolError`, the exceptions of the decoding)
  and every failure of the group loop (join, sync, heartbeat, offsets).
  - The `consume()` that is waiting throws this exception, once.
  - At once, without a call of the user: the autocommit timer stops, the
    fetch stops, a group member leaves the group, and the connections of
    the consumer are closed.
  - If no `consume()` is waiting at this moment, the next `consume()`
    throws the exception, and the calls after it throw `ConsumerClosed`.
  - An exception of the user's own code inside the `foreach` is not a
    fatal error of the consumer.
- The setup of a direct consumer is not a fatal error. A first `consume()`
  that fails or is cancelled before the consumer reads leaves it not
  started. The next `consume()` runs the setup again. No path throws
  `\LogicException('Consumer already running.')`.
- `close()` during the setup of a direct consumer wins: the consumer stays
  closed, the waiting `consume()` ends, and a later `consume()` throws
  `ConsumerClosed`.
- Busy. `consume()` throws `ConsumerBusy` at the call, as today, while
  another iterable of `consume()` holds the consumer. An iterable holds it
  from the call until its iteration ends (the end of the stream, a
  `break`, an exception) or until the iterable is destroyed. An iterable
  that is dropped without iteration frees the consumer. The `@api` doc
  and `DirectConsumerTest::concurrentConsumeThrowsConsumerBusy` do not
  change.
- Cancellation. After the cancellation of a `consume()` has fired, this
  `consume()` yields no other batch and ends without an exception, as its
  doc says. A batch that was not yielded stays in the buffer for the next
  `consume()`. No record is lost.
- After `close()` a running `consume()` yields no other batch and ends
  without an exception. Batches in the buffer are dropped.
- Poll mode, for both kinds of consumer: wait for the first batch, yield
  it, then yield every batch that is in the buffer at this moment, without
  a wait. Then end. With no batch before the cancellation the iterable is
  empty. The docs of `ConsumeMode::Poll` do not change.
- A direct consumer uses `Consumer\Config::$prefetchBufferSize` as the
  size of its buffer, as a group consumer does. A group consumer must not
  get a second buffer of this size because of it.
- New `@api` class `Thesis\Kafka\Exception\RequestTimedOut`. It extends
  `Thesis\Kafka\NetworkException`, as `Exception\ProduceTimedOut` does. It
  has no properties. The time limit that fired is its previous exception.
- A `CancelledException` in the group loop that does not come from
  `close()` is a fatal error: `consume()` throws
  `Exception\RequestTimedOut`. Only `close()` and the cancellation of the
  caller end `consume()` without an exception. This task does not change
  how many attempts a group request has (task 023).
- `ConsumerSubscription::close()` from inside the handler returns without
  a wait for the loop. The handler finishes its batch, no other batch is
  given to it, and the consumer is closed. `join()` from another coroutine
  then returns. `join()` from inside the handler is not supported and is
  not a part of this task.
- A subscription whose handle is not stored runs on. It stops only by
  `close()` or by an error. Remove the cancel from the destructor of
  `CallableHandler`. The error of such a subscription reaches the error
  handler of the event loop, as every failed future that nobody awaits.
- No `\LogicException`, `\RuntimeException` or `\Error` reaches the user
  from the paths of this task.
- Add `@throws Exception\RequestTimedOut` to `Consumer::consume()`. No
  other `@api` signature changes.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

- Dropped iterable: call `consume()` and drop the result without
  iteration. The next `consume()` reads a record. Run it for a direct
  consumer and for a group consumer.
- Busy while held: the existing
  `DirectConsumerTest::concurrentConsumeThrowsConsumerBusy` passes without
  changes. The same scenario for a group consumer.
- Cancellation with a full buffer: a topic has more batches than the
  consumer reads at once. A Stream `consume()` whose cancellation fires
  after the first batch yields nothing more. The next `consume()` reads
  the rest. Every record is read once, in order. Both kinds of consumer.
- No batch after `close()`: a direct consumer has batches in its buffer.
  `close()` is called inside the `foreach`. The loop ends without another
  batch and without an exception.
- Poll returns the buffer: several batches are in the buffer (several
  partitions, or small `maxPartitionBytes`). One Poll `consume()` yields
  more than one batch and ends without a wait for new records. Both kinds
  of consumer.
- Poll with nothing: no records. Poll returns an empty iterable after the
  time limit. The existing tests of Poll pass.
- Buffer size of a direct consumer: with `prefetchBufferSize: 1` and many
  batches in the topic, a consumer that does not read keeps a small, fixed
  number of batches. Show it through the public API, for example by the
  number of batches that one Poll returns.
- Fatal error closes: a consumer gets `UnsupportedCodec` from `consume()`
  (the setup of `FetchFailureTest`). The next `consume()` throws
  `ConsumerClosed`. `close()` after it does not throw. Both kinds of
  consumer.
- Fatal error releases: a group consumer with autocommit gets a fatal
  error. Without a `close()` no event loop work of the consumer is left
  (`loopWorkAfter()`), and the group has no members.
- Group time limit: a group consumer on `Kafka::PROXIED_BROKER` behind a
  proxy, with a short request timeout, reads a record. The proxy goes
  silent. `consume()` throws `RequestTimedOut`. The next `consume()`
  throws `ConsumerClosed`.
- `close()` before `consume()`: for both kinds, `close()` on a new
  consumer, then `consume()` throws `ConsumerClosed`. No connection is
  opened (a `brokenPeer()` or a proxy counts them).
- Failed setup, then success: a direct consumer whose first `consume()`
  fails in the setup (the broker is not reachable through a proxy). After
  the proxy is back, the next `consume()` reads a record.
- `close()` during the setup: a direct consumer behind a silent proxy.
  `consume()` waits in the setup, `close()` is called. `consume()` ends,
  and the next `consume()` throws `ConsumerClosed`.
- Subscription closed from the handler: the handler calls `close()` of the
  subscription on the first batch. `close()` returns, `join()` returns,
  and the handler got one batch.
- Subscription without a handle: `Client::consume()` whose result is not
  stored. The handler still gets a record that is produced later. The
  test stops the subscription from inside the handler.
- The other existing tests pass without changes.

## Acceptance criteria

- [ ] An iterable of `consume()` that is dropped does not leave the
      consumer busy.
- [ ] No batch is yielded after the cancellation of `consume()` has fired
      or after `close()`.
- [ ] Poll mode returns the first batch and the buffer, for both kinds of
      consumer.
- [ ] A direct consumer uses `prefetchBufferSize`.
- [ ] A fatal error closes the consumer and releases the timer, the
      connections and the group membership.
- [ ] A time limit of the group loop leaves `consume()` as
      `RequestTimedOut`, not as a normal end.
- [ ] `close()` before the first `consume()` and `close()` during the
      setup leave the consumer closed.
- [ ] A subscription can be closed from its handler and runs without a
      stored handle.
- [ ] `README.md`, section "Consumer", says what a closed consumer does,
      that a fatal error closes it, and what Poll mode returns.
- [ ] `make check` passes.

## Out of scope

- How many attempts a group request has, and a consumer that survives a
  short outage of the coordinator (task 023).
- `\LogicException` of `GroupConsumer::commitRecords()` before the first
  join, and whether a time limit of the setup of a direct consumer reaches
  the user as `CancelledException` (task 031).
- Commits: what autocommit commits, the errors of `commitUncommitted()`,
  the commit at `close()` (task 022).
- A Fetch that holds its connection at a rebalance (task 045).
- Seek and the choice of partitions (task 034).

## Depends on

- 019 Fetch errors

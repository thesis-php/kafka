# 010 Varint and int64 without copies

Type: fixes

## Goal

Decoding a response costs time in proportion to its size. Today the time
grows with the square of the size: 40,000 records in one fetch take about 18
seconds to decode, while encoding them takes 0.2 seconds. Large batches are
not usable.

## Context

- The findings are in `audit-findings.md`, section "010". Read it first. It
  has the measured numbers.
- `src/Protocol/Reader.php` copies the rest of the buffer before it reads
  each varint. A record has six or more varints, and every compact string,
  compact array and tagged field block has one.
- Varints and 64-bit integers are computed with `Thesis\Varint\BcMath` and
  `BcMath\Number` objects, in `Reader.php` and in `src/Protocol/Writer.php`.
- PHP integers are signed 64-bit. Kafka has no unsigned 64-bit field that
  needs more.
- A varint that is cut off or too long does not end in a library exception
  today.

## What to do

Read varints from the buffer in place, without a copy of the buffer, and
with integer arithmetic.

Read and write 64-bit integers with integer arithmetic too.

A varint that ends before it is complete, or that is longer than the format
allows (5 bytes for 32 bits, 10 bytes for 64 bits), is reported with a
library exception.

The bytes on the wire do not change.

## Decisions

- No public API changes. `Reader` and `Writer` keep their methods.
- Incomplete input throws `Exception\UnexpectedEof`. A varint that is too
  long throws `Exception\UnexpectedValue` or `Exception\MalformedResponse`,
  whichever fits the code around.
- The `thesis/varint` dependency is removed from `composer.json` if nothing
  uses it after the change. `ext-bcmath` too. `make deps-analyze` decides.
- This task has a measurable result. The task report gives the decode time
  for 1,000, 10,000 and 40,000 records before and after.
- The encoding stays byte for byte the same. A test proves it against values
  encoded by the old code.

## Tests

- Large fetch: a topic gets 40,000 small records in large batches. A
  consumer reads all of them within a time limit that the old code cannot
  meet (for example 5 seconds for the decoding part).
- Round trip of edge values through produce and consume: record values and
  keys of length 0, 127, 128, 16,383, 16,384; timestamps and offsets near
  the limits of 32 and 64 bits; many headers.
- Wire compatibility: for a fixed set of integers (0, 1, -1, 127, 128, the
  minimum and maximum of int32 and int64) the encoded bytes are equal to
  known byte strings from the Kafka protocol guide.
- Bad input: a response that is cut in the middle of a varint, and one with
  a varint of 11 bytes, end in a library exception. Use the same kind of
  test peer as task 015 if a real broker cannot send such bytes.

## Acceptance criteria

- [ ] All scenarios from "Tests" pass.
- [ ] The large fetch test fails without the fix. The task report shows the
      numbers before and after.
- [ ] The existing tests pass without changes.
- [ ] `make check` passes.

## Out of scope

- Other copies and allocations on the read and write paths (task 026).
- Changes to the generated protocol classes.

## Depends on

nothing

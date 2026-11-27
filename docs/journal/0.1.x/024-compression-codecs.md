# 024 Compression codecs

Type: fixes

## Goal

The client reads and writes Snappy, LZ4 and Zstd batches in the formats
that Kafka and the Java client use, and the tests prove it on a broker. A
consumer can limit the size of a decompressed batch. Today Snappy and LZ4
use formats that Kafka does not understand, no test runs these codecs, and
a small compressed batch can take any amount of memory.

## Context

- The findings are in `audit-findings.md`, section "024". They were
  checked against the code when this task was planned and are all still
  true.
- The codecs are in `src/Compressor/`: `Gzip`, `Snappy`, `Lz4`, `Zstd`,
  `None`, and the `Registry`. Each implements the `@api` interface
  `Thesis\Kafka\Compressor` (`src/Compressor.php`): `codec()`,
  `compress(string $data): string`, `decompress(string $data): string`.
  The `@api` helper functions `Compressor\compress()` and
  `Compressor\decompress()` (`src/Compressor/compression.php`) wrap a
  function of an extension.
- Snappy. `Snappy::decompress()` calls `snappy_uncompress()`, which reads
  one raw Snappy block. The Java client and the broker write the xerial
  framing: a header (the magic bytes `\x82SNAPPY\x00`, a version and a
  compatible version, 4 bytes each), then blocks. Each block is a 4-byte
  big-endian length and a raw Snappy block. Other clients (librdkafka, Go
  clients) write one raw block, and the Java client reads it.
- LZ4. `Lz4` calls `lz4_compress()` and `lz4_uncompress()`. This is the
  block format of the extension. Kafka uses the LZ4 frame format for
  record batches of the current format (magic 2). The extension
  `kjdev/php-ext-lz4` has `lz4_compress_frame()` and
  `lz4_uncompress_frame()`. `lz4_uncompress_frame()` has no size limit.
  `Lz4.php` has two `@phpstan-ignore function.notFound` comments, because
  the image has no extension.
- Zstd. `Zstd` calls `zstd_compress()` and `zstd_uncompress()`. A Java
  producer writes Zstd frames in a stream, so the frame header has no
  content size. It is not checked whether `zstd_uncompress()` reads such a
  frame. The extension `kjdev/php-ext-zstd` also has an incremental API:
  `zstd_uncompress_init()` and `zstd_uncompress_add()`.
- `Compressor\compress()` and `Compressor\decompress()` check only for an
  empty string. When the function of the extension returns `false` without
  a warning, the helper returns `false` from a function with the return
  type `string`. This is a `TypeError`.
- A consumer decompresses in
  `Consumer\Internal\Protocol\RecordBatch::decode()`
  (`src/Consumer/Internal/Protocol/RecordBatch.php`) with the codec from
  `Consumer\Config::$decompressors`. A producer compresses in
  `Producer\Internal\Protocol\RecordBatch` with
  `Producer\Config::$compressor`.
- Since tasks 019 and 020 an exception of the decoding stops the consumer:
  `consume()` throws it, and the consumer is closed.
- The test image. The `php` service of `compose.yaml` uses the image
  `ghcr.io/phpyh/php:${PHP_IMAGE_VERSION}` (Alpine). It has `zlib` and no
  `snappy`, `lz4` or `zstd`. The image has the tool
  `install-php-extensions`. CI (`.github/workflows/check.yaml`) runs the
  same `make` targets with `PHP_IMAGE_VERSION` 8.4 and 8.5, and
  `make up-test-services` already runs `docker compose up --build`.
- The only tests of the codecs are `tests/Compressor/NoneTest.php` and
  `tests/Compressor/RegistryTest.php`. `FetchFailureTest` uses `Gzip`.
- The broker can play a Java client in a test. A topic has the config
  `compression.type`. With `snappy`, `lz4` or `zstd` the broker writes
  every batch again with its own (Java) codec, so a consumer gets the
  format of Java. With another codec than the one of the producer the
  broker must first read the batch of the producer with its own codec, so
  a wrong format of the producer fails. The test kit creates such a topic:
  `$kafka->topic(configs: ['compression.type' => 'lz4'])`.
- Kafka protocol guide, record batch:
  https://kafka.apache.org/documentation/#recordbatch
- LZ4 frame format:
  https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
- xerial framing: https://github.com/xerial/snappy-java

## What to do

Give the tests a PHP image with the three extensions.

Make each codec read what a Java client writes and write what a broker
accepts. Prove both directions on a broker for every codec.

Turn every failure of a codec into a library exception.

Let a consumer set the largest size of a decompressed batch. A batch over
the limit stops the consumer with its own exception, before it takes much
more memory than the limit.

## Decisions

- Test image. Add a `Dockerfile` to the repository that starts from
  `ghcr.io/phpyh/php:${PHP_IMAGE_VERSION}` and installs `snappy`, `lz4`
  and `zstd`. The `php` service of `compose.yaml` builds from it. There is
  one image for everything: tests, PHPStan, the other tools and CI. This
  change of `compose.yaml` is agreed with the developer. Local runs and CI
  must still work with `PHP_IMAGE_VERSION` 8.4 and 8.5.
- Remove the two `@phpstan-ignore` comments from `Lz4.php`. Add no new
  ignore.
- Snappy:
  - the producer writes one raw Snappy block, as today;
  - the consumer reads both: data that starts with the xerial header is
    read block by block, everything else is read as one raw block.
- LZ4: the producer writes the LZ4 frame format and the consumer reads it.
  `Lz4::supported()` returns true only when the extension has the frame
  functions. The constructor throws `Exception\MissingExtension` when it
  does not, as for a missing extension.
- Zstd: the consumer reads a frame without a content size. If the plain
  function of the extension cannot read it, use the incremental API.
- Gzip keeps its format.
- A function of an extension that fails in any way (a warning, `false`,
  an empty result) gives `Exception\CompressionFailed`. No `\TypeError`
  reaches the user.
- The limit. New public property
  `Consumer\Config::$maxDecompressedBytes` (`?positive-int`, constructor
  parameter with the same name). The default is `null`: no limit, as today
  and as in the Java client.
- `Compressor::decompress()` changes to
  `decompress(string $data, ?int $maxBytes = null): string`, with
  `@param ?positive-int $maxBytes` (null means no limit). This change of
  an `@api` interface is agreed with the developer. Every codec of the
  library follows it, also `None`. The `@api` helper
  `Compressor\decompress()` may change in the same way.
- New `@api` class `Thesis\Kafka\Exception\DecompressedSizeExceeded`. It
  extends `Thesis\Kafka\Exception`. Public readonly properties: `codec`
  (`Thesis\Kafka\Compressor\Codec`) and `limit` (`positive-int`).
- A batch whose decompressed size is over the limit throws
  `DecompressedSizeExceeded`. A batch of exactly the limit is read. For a
  consumer this is a fatal error, as every exception of the decoding:
  `consume()` throws it and the consumer is closed.
- How strict the limit is:
  - Gzip, Snappy and Zstd must stop before or while they decompress. They
    must not build the whole output first. (`gzdecode()` has a length
    parameter, a Snappy block starts with its length, Zstd has the
    incremental API.)
  - LZ4: the extension cannot stop early. A check after the decompression
    is accepted for LZ4. Use the content size of the frame header when the
    frame has one. Write this difference into the doc of the config
    property.
- The limit is for the consumer only. `Producer\Config` and
  `Compressor::compress()` do not change.
- `composer.json` gets no new requirement. The extensions stay optional.
- Test first: write the failing test before the fix, where a test can show
  the problem.

## Tests

Run each scenario for Snappy, LZ4 and Zstd. Use a data provider. Gzip
joins where the scenario says "every codec".

- Reads the format of Java: a topic with `compression.type` of the codec.
  A producer without compression sends records. A consumer reads them with
  the same values. A raw Fetch (or the size of the answer) is not needed:
  the broker config does the work.
- Writes a format that Java reads: a topic with `compression.type=gzip`. A
  producer with the codec sends records. The produce succeeds, and a
  consumer reads the same values. The broker has read the batch with its
  own codec to write it again.
- Round trip without the broker's help: a topic with the default
  `compression.type` (`producer`). A producer with the codec sends
  records, a consumer reads the same values. Every codec.
- Large batch: the same as "Reads the format of Java" with a batch of
  several megabytes of data that compresses well, so the frame has many
  blocks. Every record is read.
- Snappy reads both forms: one consumer reads a topic that has a batch
  written by the broker (xerial) and a batch written by our producer (raw
  block) on a topic with `compression.type=producer`.
- Limit stops the consumer: a batch that is larger than
  `maxDecompressedBytes` after decompression. `consume()` throws
  `DecompressedSizeExceeded` with the codec and the limit. The next
  `consume()` throws `ConsumerClosed`. Every codec.
- Limit lets a smaller batch pass: the same records with a limit above
  their size are read. Every codec.
- No limit by default: a consumer with the default config reads the large
  batch.
- Missing codec: the existing `FetchFailureTest` passes.
- The other existing tests pass. `tests/Compressor/RegistryTest.php` may
  change where it depends on which extensions the image has. Say in the
  report what changed there and why.

## Acceptance criteria

- [ ] The test image has `snappy`, `lz4` and `zstd`, locally and in CI,
      for PHP 8.4 and 8.5.
- [ ] A consumer reads Snappy, LZ4 and Zstd batches that the broker wrote.
- [ ] The broker reads Snappy, LZ4 and Zstd batches that the producer
      wrote.
- [ ] No codec failure reaches the user as `\TypeError`.
- [ ] `Consumer\Config::$maxDecompressedBytes` stops a batch over the
      limit with `DecompressedSizeExceeded`.
- [ ] `Lz4.php` has no `@phpstan-ignore`.
- [ ] `README.md` has a section about compression: the codecs, the
      extension each one needs, and the limit. `examples/` has a runnable
      script that produces and consumes with a codec.
- [ ] `AGENTS.md` says that the `php` image is built from the
      `Dockerfile` and which extensions it has.
- [ ] `make check` passes.

## Out of scope

- The old message formats (magic 0 and 1) and the old LZ4 framing of
  Kafka for them. The client does not read these formats.
- Compression levels and other options of the codecs.
- A limit for the producer.
- A way to skip a batch that is over the limit (task 034).
- The length check of `RecordBatch::decode()` (not planned, see section
  "019" of `audit-findings.md`).

## Depends on

- 020 Consumer lifecycle

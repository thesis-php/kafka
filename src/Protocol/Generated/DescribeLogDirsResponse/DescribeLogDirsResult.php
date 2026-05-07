<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\DescribeLogDirsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class DescribeLogDirsResult implements Encodable, Decodable
{
    /**
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param string $logDir The absolute log directory path.
     * @param list<DescribeLogDirsResult\DescribeLogDirsTopic> $topics The topics.
     * @param int $totalBytes The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
     * @param int $usableBytes The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $errorCode,
        public string $logDir,
        public array $topics,
        public int $totalBytes,
        public int $usableBytes,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 2;
        $writer->writeInt16($this->errorCode);
        if ($flexible) {
            $writer->writeCompactString($this->logDir);
        } else {
            $writer->writeString($this->logDir);
        }
        if ($flexible) {
            $writer->writeCompactArrayLength(\count($this->topics));
        } else {
            $writer->writeArrayLength(\count($this->topics));
        }
        foreach ($this->topics as $item) {
            $item->encode($writer, $version);
        }
        if ($version >= 4) {
            $writer->writeInt64($this->totalBytes);
        }
        if ($version >= 4) {
            $writer->writeInt64($this->usableBytes);
        }
        if ($flexible) {
            $writer->writeTaggedFields($this->unknownTags);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 2;
        $errorCode = $reader->readInt16();
        if ($flexible) {
            $logDir = $reader->readCompactString();
        } else {
            $logDir = $reader->readString();
        }
        if ($flexible) {
            $topicsCount = $reader->readCompactArrayLength();
        } else {
            $topicsCount = $reader->readArrayLength();
        }
        $topics = [];
        for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
            $item = DescribeLogDirsResult\DescribeLogDirsTopic::decode($reader, $version);
            $topics[] = $item;
        }
        $totalBytes = -1;
        if ($version >= 4) {
            $totalBytes = $reader->readInt64();
        }
        $usableBytes = -1;
        if ($version >= 4) {
            $usableBytes = $reader->readInt64();
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
        } else {
            $unknownTags = new UnknownTags();
        }
        return new self($errorCode, $logDir, $topics, $totalBytes, $usableBytes, $unknownTags);
    }
}

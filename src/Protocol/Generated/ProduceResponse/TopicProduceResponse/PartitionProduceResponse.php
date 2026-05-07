<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\ProduceResponse\TopicProduceResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class PartitionProduceResponse implements Encodable, Decodable
{
    /**
     * @param int $index The partition index.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param int $baseOffset The base offset.
     * @param int $logAppendTimeMs The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
     * @param int $logStartOffset The log start offset.
     * @param list<PartitionProduceResponse\BatchIndexAndErrorMessage> $recordErrors The batch indices of records that caused the batch to be dropped.
     * @param ?string $errorMessage The global error message summarizing the common root cause of the records that caused the batch to be dropped.
     * @param PartitionProduceResponse\LeaderIdAndEpoch $currentLeader The leader broker that the producer should use for future requests.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $index,
        public int $errorCode,
        public int $baseOffset,
        public int $logAppendTimeMs,
        public int $logStartOffset,
        public array $recordErrors,
        public ?string $errorMessage,
        public PartitionProduceResponse\LeaderIdAndEpoch $currentLeader,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 9;
        $writer->writeInt32($this->index);
        $writer->writeInt16($this->errorCode);
        $writer->writeInt64($this->baseOffset);
        if ($version >= 2) {
            $writer->writeInt64($this->logAppendTimeMs);
        }
        if ($version >= 5) {
            $writer->writeInt64($this->logStartOffset);
        }
        if ($version >= 8) {
            if ($flexible) {
                $writer->writeCompactArrayLength(\count($this->recordErrors));
            } else {
                $writer->writeArrayLength(\count($this->recordErrors));
            }
            foreach ($this->recordErrors as $item) {
                $item->encode($writer, $version);
            }
        }
        if ($version >= 8) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->errorMessage);
            } else {
                $writer->writeNullableString($this->errorMessage);
            }
        }
        if ($flexible) {
            $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
            if ($version >= 10) {
                if (isset($taggedFields[0])) {
                    throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'CurrentLeader\'.');
                }
                $tagWriter = new Writer();
                $this->currentLeader->encode($tagWriter, $version);
                $taggedFields[0] = (string) $tagWriter;
            }
            $writer->writeTaggedFields($taggedFields);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 9;
        $index = $reader->readInt32();
        $errorCode = $reader->readInt16();
        $baseOffset = $reader->readInt64();
        $logAppendTimeMs = -1;
        if ($version >= 2) {
            $logAppendTimeMs = $reader->readInt64();
        }
        $logStartOffset = -1;
        if ($version >= 5) {
            $logStartOffset = $reader->readInt64();
        }
        $recordErrors = [];
        if ($version >= 8) {
            if ($flexible) {
                $recordErrorsCount = $reader->readCompactArrayLength();
            } else {
                $recordErrorsCount = $reader->readArrayLength();
            }
            $recordErrors = [];
            for ($recordErrorsIndex = 0; $recordErrorsIndex < $recordErrorsCount; ++$recordErrorsIndex) {
                $item = PartitionProduceResponse\BatchIndexAndErrorMessage::decode($reader, $version);
                $recordErrors[] = $item;
            }
        }
        $errorMessage = null;
        if ($version >= 8) {
            if ($flexible) {
                $errorMessage = $reader->readCompactNullableString();
            } else {
                $errorMessage = $reader->readNullableString();
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if ($version >= 10) {
                if (isset($unknownTags[0])) {
                    $tagReader = new Reader($unknownTags[0]);
                    $currentLeader = PartitionProduceResponse\LeaderIdAndEpoch::decode($tagReader, $version);
                    unset($unknownTags[0]);
                } else {
                    $currentLeader = new PartitionProduceResponse\LeaderIdAndEpoch(-1, -1);
                }
            } else {
                $currentLeader = new PartitionProduceResponse\LeaderIdAndEpoch(-1, -1);
            }
        } else {
            $unknownTags = new UnknownTags();
            $currentLeader = new PartitionProduceResponse\LeaderIdAndEpoch(-1, -1);
        }
        return new self($index, $errorCode, $baseOffset, $logAppendTimeMs, $logStartOffset, $recordErrors, $errorMessage, $currentLeader, $unknownTags);
    }
}

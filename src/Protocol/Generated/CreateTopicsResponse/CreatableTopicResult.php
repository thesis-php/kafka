<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated\CreateTopicsResponse;

use Thesis\Kafka\Protocol\Decodable;
use Thesis\Kafka\Protocol\Encodable;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 */
final readonly class CreatableTopicResult implements Encodable, Decodable
{
    /**
     * @param string $name The topic name.
     * @param string $topicId The unique topic ID.
     * @param int $errorCode The error code, or 0 if there was no error.
     * @param ?string $errorMessage The error message, or null if there was no error.
     * @param int $topicConfigErrorCode Optional topic config error returned if configs are not returned in the response.
     * @param int $numPartitions Number of partitions of the topic.
     * @param int $replicationFactor Replication factor of the topic.
     * @param ?list<CreatableTopicResult\CreatableTopicConfigs> $configs Configuration of the topic.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public string $name,
        public string $topicId,
        public int $errorCode,
        public ?string $errorMessage,
        public int $topicConfigErrorCode,
        public int $numPartitions,
        public int $replicationFactor,
        public ?array $configs,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $writer->writeCompactString($this->name);
        } else {
            $writer->writeString($this->name);
        }
        if ($version >= 7) {
            $writer->writeUuid($this->topicId);
        }
        $writer->writeInt16($this->errorCode);
        if ($version >= 1) {
            if ($flexible) {
                $writer->writeCompactNullableString($this->errorMessage);
            } else {
                $writer->writeNullableString($this->errorMessage);
            }
        }
        if ($version >= 5) {
            $writer->writeInt32($this->numPartitions);
        }
        if ($version >= 5) {
            $writer->writeInt16($this->replicationFactor);
        }
        if ($version >= 5) {
            $writer->writeCompactNullableArrayLength($this->configs === null ? null : \count($this->configs));
            if ($this->configs !== null) {
                foreach ($this->configs as $item) {
                    $item->encode($writer, $version);
                }
            }
        }
        if ($flexible) {
            $taggedFields = new UnknownTags(iterator_to_array($this->unknownTags));
            if (isset($taggedFields[0])) {
                throw new \UnexpectedValueException('Unknown tag \'0\' collides with known field \'TopicConfigErrorCode\'.');
            }
            $tagWriter = new Writer();
            $tagWriter->writeInt16($this->topicConfigErrorCode);
            $taggedFields[0] = (string) $tagWriter;
            $writer->writeTaggedFields($taggedFields);
        }
    }

    #[\Override]
    public static function decode(Reader $reader, int $version): static
    {
        $flexible = $version >= 5;
        if ($flexible) {
            $name = $reader->readCompactString();
        } else {
            $name = $reader->readString();
        }
        $topicId = '';
        if ($version >= 7) {
            $topicId = $reader->readUuid();
        }
        $errorCode = $reader->readInt16();
        $errorMessage = null;
        if ($version >= 1) {
            if ($flexible) {
                $errorMessage = $reader->readCompactNullableString();
            } else {
                $errorMessage = $reader->readNullableString();
            }
        }
        $numPartitions = -1;
        if ($version >= 5) {
            $numPartitions = $reader->readInt32();
        }
        $replicationFactor = -1;
        if ($version >= 5) {
            $replicationFactor = $reader->readInt16();
        }
        $configs = null;
        if ($version >= 5) {
            $configsCount = $reader->readCompactNullableArrayLength();
            if ($configsCount === null) {
                $configs = null;
            } else {
                $configs = [];
                for ($configsIndex = 0; $configsIndex < $configsCount; ++$configsIndex) {
                    $item = CreatableTopicResult\CreatableTopicConfigs::decode($reader, $version);
                    $configs[] = $item;
                }
            }
        }
        if ($flexible) {
            $unknownTags = $reader->readTaggedFields();
            if (isset($unknownTags[0])) {
                $tagReader = new Reader($unknownTags[0]);
                $topicConfigErrorCode = $tagReader->readInt16();
                unset($unknownTags[0]);
            } else {
                $topicConfigErrorCode = 0;
            }
        } else {
            $unknownTags = new UnknownTags();
            $topicConfigErrorCode = 0;
        }
        return new self($name, $topicId, $errorCode, $errorMessage, $topicConfigErrorCode, $numPartitions, $replicationFactor, $configs, $unknownTags);
    }
}

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Generator\Generator\NamespaceGenerator;
use Thesis\Kafka\Generator\SchemaSource\StaticSource;
use Thesis\Kafka\Generator\Writer\InMemoryWriter;

final class GeneratorTest
{
    #[Test]
    public function generatesHeaderWithFieldLevelFlexibleOverride(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "header",
                  "name": "RequestHeader",
                  "validVersions": "1-2",
                  "flexibleVersions": "2+",
                  "fields": [
                    {
                      "name": "RequestApiKey",
                      "type": "int16",
                      "versions": "1+",
                      "about": "The API key of this request."
                    },
                    {
                      "name": "ClientId",
                      "type": "string",
                      "versions": "1+",
                      "nullableVersions": "1+",
                      "flexibleVersions": "none",
                      "about": "The client ID string."
                    }
                  ]
                }
                JSON,
            [
                'RequestHeader.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Decodable;
                    use Thesis\Kafka\Protocol\Encodable;
                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\UnknownTags;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class RequestHeader implements Encodable, Decodable
                    {
                        /**
                         * @param int $requestApiKey The API key of this request.
                         * @param ?string $clientId The client ID string.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public int $requestApiKey,
                            public ?string $clientId,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $flexible = $version >= 2;
                            $writer->writeInt16($this->requestApiKey);
                            $writer->writeNullableString($this->clientId);
                            if ($flexible) {
                                $writer->writeTaggedFields($this->unknownTags);
                            }
                        }

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $flexible = $version >= 2;
                            $requestApiKey = $reader->readInt16();
                            $clientId = $reader->readNullableString();
                            if ($flexible) {
                                $unknownTags = $reader->readTaggedFields();
                            } else {
                                $unknownTags = new UnknownTags();
                            }
                            return new self($requestApiKey, $clientId, $unknownTags);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesRequestWithInlineAndCommonStructs(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "request",
                  "name": "SampleRequest",
                  "apiKey": 42,
                  "listeners": ["broker"],
                  "validVersions": "0-1",
                  "flexibleVersions": "1+",
                  "fields": [
                    {
                      "name": "Filter",
                      "type": "Filter",
                      "versions": "0+",
                      "about": "Common filter."
                    },
                    {
                      "name": "Topics",
                      "type": "[]Topic",
                      "versions": "0+",
                      "about": "Inline topic list.",
                      "fields": [
                        {
                          "name": "Name",
                          "type": "string",
                          "versions": "0+",
                          "about": "Topic name."
                        }
                      ]
                    }
                  ],
                  "commonStructs": [
                    {
                      "name": "Filter",
                      "versions": "0+",
                      "fields": [
                        {
                          "name": "Pattern",
                          "type": "string",
                          "versions": "0+",
                          "nullableVersions": "0+",
                          "about": "Nullable pattern."
                        }
                      ]
                    }
                  ]
                }
                JSON,
            [
                'Listener.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    /**
                     * @api
                     * @generated
                     */
                    enum Listener: string
                    {
                        case Broker = 'broker';
                    }

                    PHP,
                'ApiKey.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\VersionRange;

                    /**
                     * @api
                     * @generated
                     */
                    enum ApiKey: int
                    {
                        case Sample = 42;

                        /**
                         * @return list<Listener>
                         */
                        public function listeners(): array
                        {
                            return match ($this) {
                                self::Sample => [Listener::Broker],
                            };
                        }

                        public function requiresAnyListener(Listener ...$listeners): bool
                        {
                            return array_any($listeners, fn(Listener $listener) => \in_array($listener, $this->listeners(), true));
                        }

                        public function clientVersionRange(): VersionRange
                        {
                            return match ($this) {
                                self::Sample => new VersionRange(0, 1),
                            };
                        }

                        public function isFlexibleVersion(int $apiVersion): bool
                        {
                            return match ($this) {
                                self::Sample => $apiVersion >= 1,
                            };
                        }

                        /**
                         * @return non-negative-int
                         */
                        public function requestHeaderVersion(int $apiVersion): int
                        {
                            return $this->isFlexibleVersion($apiVersion) ? 2 : 1;
                        }

                        /**
                         * @return non-negative-int
                         */
                        public function responseHeaderVersion(int $apiVersion): int
                        {
                            return $this->isFlexibleVersion($apiVersion) ? 1 : 0;
                        }
                    }

                    PHP,
                'SampleRequest.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\UnknownTags;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     * @template-implements Request<SampleResponse>
                     */
                    final readonly class SampleRequest implements Request
                    {
                        /**
                         * @param SampleRequest\Filter $filter Common filter.
                         * @param list<SampleRequest\Topic> $topics Inline topic list.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public SampleRequest\Filter $filter,
                            public array $topics,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public function apiKey(): ApiKey
                        {
                            return ApiKey::Sample;
                        }

                        #[\Override]
                        public function responseType(): string
                        {
                            return SampleResponse::class;
                        }

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $flexible = $version >= 1;
                            $this->filter->encode($writer, $version);
                            if ($flexible) {
                                $writer->writeCompactArrayLength(\count($this->topics));
                            } else {
                                $writer->writeArrayLength(\count($this->topics));
                            }
                            foreach ($this->topics as $item) {
                                $item->encode($writer, $version);
                            }
                            if ($flexible) {
                                $writer->writeTaggedFields($this->unknownTags);
                            }
                        }
                    }

                    PHP,
                'SampleRequest/Filter.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated\SampleRequest;

                    use Thesis\Kafka\Protocol\Decodable;
                    use Thesis\Kafka\Protocol\Encodable;
                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\UnknownTags;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class Filter implements Encodable, Decodable
                    {
                        /**
                         * @param ?string $pattern Nullable pattern.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public ?string $pattern,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $flexible = $version >= 1;
                            if ($flexible) {
                                $writer->writeCompactNullableString($this->pattern);
                            } else {
                                $writer->writeNullableString($this->pattern);
                            }
                            if ($flexible) {
                                $writer->writeTaggedFields($this->unknownTags);
                            }
                        }

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $flexible = $version >= 1;
                            if ($flexible) {
                                $pattern = $reader->readCompactNullableString();
                            } else {
                                $pattern = $reader->readNullableString();
                            }
                            if ($flexible) {
                                $unknownTags = $reader->readTaggedFields();
                            } else {
                                $unknownTags = new UnknownTags();
                            }
                            return new self($pattern, $unknownTags);
                        }
                    }

                    PHP,
                'SampleRequest/Topic.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated\SampleRequest;

                    use Thesis\Kafka\Protocol\Decodable;
                    use Thesis\Kafka\Protocol\Encodable;
                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\UnknownTags;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class Topic implements Encodable, Decodable
                    {
                        /**
                         * @param string $name Topic name.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public string $name,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $flexible = $version >= 1;
                            if ($flexible) {
                                $writer->writeCompactString($this->name);
                            } else {
                                $writer->writeString($this->name);
                            }
                            if ($flexible) {
                                $writer->writeTaggedFields($this->unknownTags);
                            }
                        }

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $flexible = $version >= 1;
                            if ($flexible) {
                                $name = $reader->readCompactString();
                            } else {
                                $name = $reader->readString();
                            }
                            if ($flexible) {
                                $unknownTags = $reader->readTaggedFields();
                            } else {
                                $unknownTags = new UnknownTags();
                            }
                            return new self($name, $unknownTags);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesVersionGatedNullableScalar(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "request",
                  "name": "NullableGateRequest",
                  "apiKey": 7,
                  "validVersions": "0-1",
                  "fields": [
                    {
                      "name": "Name",
                      "type": "string",
                      "versions": "0+",
                      "nullableVersions": "1+",
                      "about": "Version-gated nullable name."
                    }
                  ]
                }
                JSON,
            [
                'NullableGateRequest.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     * @template-implements Request<NullableGateResponse>
                     */
                    final readonly class NullableGateRequest implements Request
                    {
                        /**
                         * @param ?string $name Version-gated nullable name.
                         */
                        public function __construct(
                            public ?string $name,
                        ) {}

                        #[\Override]
                        public function apiKey(): ApiKey
                        {
                            return ApiKey::NullableGate;
                        }

                        #[\Override]
                        public function responseType(): string
                        {
                            return NullableGateResponse::class;
                        }

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            if ($version >= 1) {
                                $writer->writeNullableString($this->name);
                            } else {
                                if ($this->name === null) {
                                    throw new \UnexpectedValueException("Field 'Name' must not be null in version '{$version}'");
                                }
                                $writer->writeString($this->name);
                            }
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function resolvesFlexibilityForEachNullableBranch(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "response",
                  "name": "NullableFlexibleGateResponse",
                  "apiKey": 7,
                  "validVersions": "0-3",
                  "flexibleVersions": "2+",
                  "fields": [
                    {
                      "name": "Name",
                      "type": "string",
                      "versions": "0+",
                      "nullableVersions": "3+",
                      "about": "Nullable only in a flexible version."
                    },
                    {
                      "name": "Topics",
                      "type": "[]string",
                      "versions": "0+",
                      "nullableVersions": "1+",
                      "about": "Not nullable only in a non-flexible version."
                    }
                  ]
                }
                JSON,
            [
                'NullableFlexibleGateResponse.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\UnknownTags;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class NullableFlexibleGateResponse implements Response
                    {
                        /**
                         * @param ?string $name Nullable only in a flexible version.
                         * @param ?list<string> $topics Not nullable only in a non-flexible version.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public ?string $name,
                            public ?array $topics,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $flexible = $version >= 2;
                            if ($version >= 3) {
                                $name = $reader->readCompactNullableString();
                            } elseif ($flexible) {
                                $name = $reader->readCompactString();
                            } else {
                                $name = $reader->readString();
                            }
                            if ($version >= 1) {
                                if ($flexible) {
                                    $topicsCount = $reader->readCompactNullableArrayLength();
                                } else {
                                    $topicsCount = $reader->readNullableArrayLength();
                                }
                                if ($topicsCount === null) {
                                    $topics = null;
                                } else {
                                    $topics = [];
                                    for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
                                        if ($flexible) {
                                            $item = $reader->readCompactString();
                                        } else {
                                            $item = $reader->readString();
                                        }
                                        $topics[] = $item;
                                    }
                                }
                            } else {
                                $topicsCount = $reader->readArrayLength();
                                $topics = [];
                                for ($topicsIndex = 0; $topicsIndex < $topicsCount; ++$topicsIndex) {
                                    if ($flexible) {
                                        $item = $reader->readCompactString();
                                    } else {
                                        $item = $reader->readString();
                                    }
                                    $topics[] = $item;
                                }
                            }
                            if ($flexible) {
                                $unknownTags = $reader->readTaggedFields();
                            } else {
                                $unknownTags = new UnknownTags();
                            }
                            return new self($name, $topics, $unknownTags);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesNullableArraysAndRecords(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "response",
                  "name": "PayloadResponse",
                  "validVersions": "0+",
                  "fields": [
                    {
                      "name": "Ids",
                      "type": "[]int32",
                      "versions": "0+",
                      "nullableVersions": "0+",
                      "about": "Nullable ids."
                    },
                    {
                      "name": "Payload",
                      "type": "records",
                      "versions": "0+",
                      "nullableVersions": "0+",
                      "about": "Nullable record batch."
                    }
                  ]
                }
                JSON,
            [
                'PayloadResponse.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Reader;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class PayloadResponse implements Response
                    {
                        /**
                         * @param ?list<int> $ids Nullable ids.
                         * @param ?string $payload Nullable record batch.
                         */
                        public function __construct(
                            public ?array $ids,
                            public ?string $payload,
                        ) {}

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $idsCount = $reader->readNullableArrayLength();
                            if ($idsCount === null) {
                                $ids = null;
                            } else {
                                $ids = [];
                                for ($idsIndex = 0; $idsIndex < $idsCount; ++$idsIndex) {
                                    $item = $reader->readInt32();
                                    $ids[] = $item;
                                }
                            }
                            $payload = $reader->readNullableBytes();
                            return new self($ids, $payload);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesDefaultsForDifferentScalarTypes(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "response",
                  "name": "DefaultsResponse",
                  "validVersions": "0-1",
                  "fields": [
                    {
                      "name": "ErrorCode",
                      "type": "int16",
                      "versions": "0+",
                      "about": "Top-level error."
                    },
                    {
                      "name": "RetryCount",
                      "type": "int32",
                      "versions": "1+",
                      "default": -1,
                      "about": "Retry count."
                    },
                    {
                      "name": "Label",
                      "type": "string",
                      "versions": "1+",
                      "default": "foo",
                      "about": "Default label."
                    },
                    {
                      "name": "Enabled",
                      "type": "bool",
                      "versions": "1+",
                      "default": false,
                      "about": "Default flag."
                    }
                  ]
                }
                JSON,
            [
                'DefaultsResponse.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Reader;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class DefaultsResponse implements Response
                    {
                        /**
                         * @param int $errorCode Top-level error.
                         * @param int $retryCount Retry count.
                         * @param string $label Default label.
                         * @param bool $enabled Default flag.
                         */
                        public function __construct(
                            public int $errorCode,
                            public int $retryCount,
                            public string $label,
                            public bool $enabled,
                        ) {}

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $errorCode = $reader->readInt16();
                            $retryCount = -1;
                            if ($version >= 1) {
                                $retryCount = $reader->readInt32();
                            }
                            $label = 'foo';
                            if ($version >= 1) {
                                $label = $reader->readString();
                            }
                            $enabled = false;
                            if ($version >= 1) {
                                $enabled = $reader->readBool();
                            }
                            return new self($errorCode, $retryCount, $label, $enabled);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesNestedInlineStructs(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "response",
                  "name": "NestedResponse",
                  "validVersions": "0+",
                  "fields": [
                    {
                      "name": "Outer",
                      "type": "Outer",
                      "versions": "0+",
                      "about": "Outer struct.",
                      "fields": [
                        {
                          "name": "Inner",
                          "type": "Inner",
                          "versions": "0+",
                          "about": "Inner struct.",
                          "fields": [
                            {
                              "name": "Value",
                              "type": "string",
                              "versions": "0+",
                              "about": "Nested value."
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
                JSON,
            [
                'NestedResponse/Outer.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated\NestedResponse;

                    use Thesis\Kafka\Protocol\Decodable;
                    use Thesis\Kafka\Protocol\Encodable;
                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class Outer implements Encodable, Decodable
                    {
                        /**
                         * @param Outer\Inner $inner Inner struct.
                         */
                        public function __construct(
                            public Outer\Inner $inner,
                        ) {}

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $this->inner->encode($writer, $version);
                        }

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $inner = Outer\Inner::decode($reader, $version);
                            return new self($inner);
                        }
                    }

                    PHP,
                'NestedResponse/Outer/Inner.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated\NestedResponse\Outer;

                    use Thesis\Kafka\Protocol\Decodable;
                    use Thesis\Kafka\Protocol\Encodable;
                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\Writer;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class Inner implements Encodable, Decodable
                    {
                        /**
                         * @param string $value Nested value.
                         */
                        public function __construct(
                            public string $value,
                        ) {}

                        #[\Override]
                        public function encode(Writer $writer, int $version): void
                        {
                            $writer->writeString($this->value);
                        }

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $value = $reader->readString();
                            return new self($value);
                        }
                    }

                    PHP,
            ],
        );
    }

    #[Test]
    public function generatesResponseWithKnownTaggedFieldsAndDefaults(): void
    {
        self::assertSameContent(
            <<<'JSON'
                {
                  "type": "response",
                  "name": "TaggedResponse",
                  "validVersions": "0+",
                  "flexibleVersions": "0+",
                  "fields": [
                    {
                      "name": "ErrorCode",
                      "type": "int16",
                      "versions": "0+",
                      "about": "Top-level error."
                    },
                    {
                      "name": "ThrottleTimeMs",
                      "type": "int32",
                      "versions": "0+",
                      "taggedVersions": "0+",
                      "tag": 0,
                      "default": 0,
                      "about": "Tagged throttle."
                    },
                    {
                      "name": "Meta",
                      "type": "Meta",
                      "versions": "0+",
                      "taggedVersions": "0+",
                      "tag": 1,
                      "about": "Tagged meta.",
                      "fields": [
                        {
                          "name": "LeaderId",
                          "type": "int32",
                          "versions": "0+",
                          "default": -1,
                          "about": "Leader id."
                        }
                      ]
                    }
                  ]
                }
                JSON,
            [
                'TaggedResponse.php' => <<<'PHP'
                    <?php

                    declare(strict_types=1);

                    namespace Thesis\Kafka\Protocol\Generated;

                    use Thesis\Kafka\Protocol\Reader;
                    use Thesis\Kafka\Protocol\UnknownTags;

                    /**
                     * @api
                     * @generated
                     */
                    final readonly class TaggedResponse implements Response
                    {
                        /**
                         * @param int $errorCode Top-level error.
                         * @param int $throttleTimeMs Tagged throttle.
                         * @param TaggedResponse\Meta $meta Tagged meta.
                         * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
                         */
                        public function __construct(
                            public int $errorCode,
                            public int $throttleTimeMs,
                            public TaggedResponse\Meta $meta,
                            public UnknownTags $unknownTags = new UnknownTags(),
                        ) {}

                        #[\Override]
                        public static function decode(Reader $reader, int $version): static
                        {
                            $errorCode = $reader->readInt16();
                            $unknownTags = $reader->readTaggedFields();
                            if (isset($unknownTags[0])) {
                                $tagReader = new Reader($unknownTags[0]);
                                $throttleTimeMs = $tagReader->readInt32();
                                unset($unknownTags[0]);
                            } else {
                                $throttleTimeMs = 0;
                            }
                            if (isset($unknownTags[1])) {
                                $tagReader = new Reader($unknownTags[1]);
                                $meta = TaggedResponse\Meta::decode($tagReader, $version);
                                unset($unknownTags[1]);
                            } else {
                                $meta = new TaggedResponse\Meta(-1);
                            }
                            return new self($errorCode, $throttleTimeMs, $meta, $unknownTags);
                        }
                    }

                    PHP,
            ],
        );
    }

    /**
     * @param non-empty-string $json
     * @param array<non-empty-string, non-empty-string> $classes
     */
    private static function assertSameContent(
        string $json,
        array $classes,
    ): void {
        $writer = new InMemoryWriter();

        $generator = new Generator(
            fs: $writer,
            protocolNs: new NamespaceGenerator('Thesis\Kafka\Protocol'),
            generatedNs: new NamespaceGenerator('Thesis\Kafka\Protocol\Generated'),
        );

        $generator->generate(new StaticSource([$json]));

        foreach ($classes as $path => $class) {
            Assert::same($class, $writer->get($path));
        }
    }
}

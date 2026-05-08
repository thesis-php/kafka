<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator;

use ColinODell\Json5\SyntaxError;
use CuyZ\Valinor\Mapper\Source\Source;
use CuyZ\Valinor\Mapper\TreeMapper;
use CuyZ\Valinor\MapperBuilder;
use Nette\PhpGenerator\ClassType;
use Nette\PhpGenerator\EnumType;
use Nette\PhpGenerator\InterfaceType;
use Nette\PhpGenerator\Literal;
use Nette\PhpGenerator\Method;
use Nette\PhpGenerator\Parameter;
use Nette\PhpGenerator\PhpNamespace;
use PhpParser\Node\Expr;
use PhpParser\Node\Stmt;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Generator\Generator\Ast;
use Thesis\Kafka\Generator\Schema\ApiKeyListenerGroup;

/**
 * @internal
 */
final readonly class Generator
{
    private const array SCALARS = [
        'int8' => 1,
        'int16' => 1,
        'int32' => 1,
        'int64' => 1,
        'uint16' => 1,
        'uint32' => 1,
        'float64' => 1,
        'bool' => 1,
        'uuid' => 1,
    ];
    private const array PREFIXED = [
        'string' => 1,
        'bytes' => 1,
        'records' => 1,
    ];

    private TreeMapper $mapper;

    public function __construct(
        private Writer $fs,
        private Generator\NamespaceGenerator $protocolNs,
        private Generator\NamespaceGenerator $generatedNs,
        ?TreeMapper $mapper = null,
    ) {
        $this->mapper = $mapper ?? new MapperBuilder()
            ->allowSuperfluousKeys()
            ->allowPermissiveTypes()
            ->mapper();
    }

    public function generate(SchemaSource $input): void
    {
        $this->generateRequestInterface();
        $this->generateResponseInterface();

        $apiKeys = new Schema\ApiKeys();
        $listeners = [];

        foreach ($input->schemas() as $schemaJson) {
            $schema = self::json5decode($schemaJson);

            $message = $this->mapper->map(
                Schema\Message::class,
                Source::array($schema),
            );

            // Only client wire protocol: requests, responses and header.
            if (!\in_array($message->type, [Schema\Type::Request, Schema\Type::Response, Schema\Type::Header], true)) {
                continue;
            }

            if ($message->validVersions->isNone()) {
                continue;
            }

            if ($message->type === Schema\Type::Request) {
                $this->generateRequest($message);

                if ($message->apiKey !== null) {
                    \assert($message->validVersions->min !== null && $message->validVersions->max !== null);

                    $name = substr($message->name, 0, -7);
                    \assert($name !== '');

                    $listenersForKey = $message->listeners;
                    sort($listenersForKey);

                    $apiKeys = $apiKeys->append(new Schema\ApiKey(
                        name: $name,
                        value: $message->apiKey,
                        min: $message->validVersions->min,
                        max: $message->validVersions->max,
                        flexibleVersions: $message->flexibleVersions,
                        listeners: $listenersForKey,
                    ));

                    foreach ($listenersForKey as $listener) {
                        $listeners[$listener] = true;
                    }
                }
            } elseif ($message->type === Schema\Type::Response) {
                $this->generateResponse($message);
            } else {
                $this->generateHeader($message);
            }
        }

        $this->generateListener(array_keys($listeners));
        $this->generateApiKey($apiKeys);
    }

    private function generateRequestInterface(): void
    {
        $interface = new InterfaceType('Request')
            ->setExtends([
                $this->protocolNs->fqcn('Encodable'),
            ])
            ->addComment('@api')
            ->addComment('@generated')
            ->addComment('@template T of Response');

        $interface
            ->addMethod('apiKey')
            ->setPublic()
            ->setReturnType($this->generatedNs->fqcn('ApiKey'));

        $interface
            ->addMethod('responseType')
            ->setPublic()
            ->setReturnType('string')
            ->setComment('@return class-string<T>');

        $namespace = $this->generatedNs
            ->new()
            ->addUse($this->protocolNs->fqcn('Encodable'))
            ->add($interface);

        $this->fs->write('Request', $namespace);
    }

    private function generateResponseInterface(): void
    {
        $interface = new InterfaceType('Response')
            ->setExtends([
                $this->protocolNs->fqcn('Decodable'),
            ])
            ->addComment('@api')
            ->addComment('@generated');

        $namespace = $this->generatedNs
            ->new()
            ->addUse($this->protocolNs->fqcn('Decodable'))
            ->add($interface);

        $this->fs->write('Response', $namespace);
    }

    private function generateRequest(Schema\Message $request): void
    {
        if ($request->validVersions->isNone()) {
            return;
        }

        $this->generateRequestClass($request);

        foreach ($request->fields as $field) {
            $this->generateInlineStructs($field, [$request->name], $request->flexibleVersions);
        }

        foreach ($request->commonStructs as $struct) {
            $this->generateCommonStruct($struct, $request, $request->flexibleVersions);
        }
    }

    private function generateResponse(Schema\Message $response): void
    {
        if ($response->validVersions->isNone()) {
            return;
        }

        $this->generateResponseClass($response);

        foreach ($response->fields as $field) {
            $this->generateInlineStructs($field, [$response->name], $response->flexibleVersions);
        }

        foreach ($response->commonStructs as $struct) {
            $this->generateCommonStruct($struct, $response, $response->flexibleVersions);
        }
    }

    private function generateHeader(Schema\Message $header): void
    {
        if ($header->validVersions->isNone()) {
            return;
        }

        $namespace = $this->generatedNamespace(
            uses: [
                $this->protocolNs->fqcn('Writer'),
                $this->protocolNs->fqcn('Reader'),
                $this->protocolNs->fqcn('Encodable'),
                $this->protocolNs->fqcn('Decodable'),
                ...(!$header->flexibleVersions->isNone() ? [$this->protocolNs->fqcn('UnknownTags')] : []),
            ],
        );

        if ($this->usesUuidFactory($header->fields, $header->validVersions)) {
            $namespace->addUse(Uuid::class);
        }

        $class = $this->generatedClass(
            $header->name,
            [
                $this->protocolNs->fqcn('Encodable'),
                $this->protocolNs->fqcn('Decodable'),
            ],
        )
            ->setMethods([
                $this->generateConstructor($namespace, $header->fields, [$header->name], $header->flexibleVersions),
                $this->generateEncodeMethod($header->fields, $header->validVersions, $header->flexibleVersions),
                $this->generateDecodeMethod($header->fields, $header->validVersions, $header->flexibleVersions, [$header->name], $namespace),
            ]);

        $namespace->add($class);

        $this->fs->write($header->name, $namespace);

        foreach ($header->fields as $field) {
            $this->generateInlineStructs($field, [$header->name], $header->flexibleVersions);
        }

        foreach ($header->commonStructs as $struct) {
            $this->generateCommonStruct($struct, $header, $header->flexibleVersions);
        }
    }

    private function generateApiKey(Schema\ApiKeys $keys): void
    {
        $enum = new EnumType('ApiKey')
            ->setType('int')
            ->addComment('@api')
            ->addComment('@generated');

        foreach ($keys as $key) {
            $enum->addCase($key->name, $key->value);
        }

        $enum
            ->addMethod('listeners')
            ->setPublic()
            ->setReturnType('array')
            ->setBody(Ast::body(
                Ast::return(
                    Ast::match(
                        Ast::this(),
                        array_map(
                            static fn(ApiKeyListenerGroup $group) => Ast::armMany(
                                array_map(
                                    static fn(Schema\ApiKey $key) => Ast::classConst(class: 'self', name: $key->name),
                                    $group->keys,
                                ),
                                Ast::array(
                                    array_map(
                                        static fn(string $listener) => Ast::classConst(class: 'Listener', name: self::listenerCaseName($listener)),
                                        $group->listeners,
                                    ),
                                ),
                            ),
                            $keys->groupListeners(),
                        ),
                    ),
                ),
            ))
            ->addComment('@return list<Listener>');

        $enum
            ->addMethod('requiresAnyListener')
            ->setPublic()
            ->setReturnType('bool')
            ->setVariadic()
            ->setParameters([
                new Parameter('listeners')
                    ->setType($this->generatedNs->fqcn('Listener')),
            ])
            ->setBody(Ast::body(
                Ast::return(
                    Ast::funcCall(
                        'array_any',
                        Ast::var('listeners'),
                        Ast::fn(
                            'listener',
                            'Listener',
                            Ast::funcCall(
                                '\in_array',
                                Ast::var('listener'),
                                Ast::call(Ast::this(), 'listeners'),
                                Ast::bool(true),
                            ),
                        ),
                    ),
                ),
            ));

        $enum
            ->addMethod('clientVersionRange')
            ->setPublic()
            ->setReturnType($this->protocolNs->fqcn('VersionRange'))
            ->setBody(Ast::body(
                Ast::return(
                    Ast::match(
                        Ast::this(),
                        array_map(
                            static fn(Schema\ApiKeyVersionRangeGroup $group) => Ast::armMany(
                                array_map(
                                    static fn(Schema\ApiKey $key) => Ast::classConst('self', $key->name),
                                    $group->keys,
                                ),
                                Ast::new('VersionRange', Ast::int($group->min), Ast::int($group->max)),
                            ),
                            $keys->groupVersionRanges(),
                        ),
                    ),
                ),
            ));

        $enum
            ->addMethod('isFlexibleVersion')
            ->setPublic()
            ->setReturnType('bool')
            ->setParameters([
                new Parameter('apiVersion')->setType('int'),
            ])
            ->setBody(Ast::body(
                Ast::return(
                    Ast::match(
                        Ast::this(),
                        array_map(
                            static fn(Schema\ApiKeyFlexibleVersionGroup $group) => Ast::armMany(
                                array_map(
                                    static fn(Schema\ApiKey $key): Expr => Ast::classConst('self', $key->name),
                                    $group->keys,
                                ),
                                self::versionContainsExpr($group->flexibleVersions, Ast::var('apiVersion')),
                            ),
                            $keys->groupFlexibleVersions(),
                        ),
                    ),
                ),
            ));

        $enum
            ->addMethod('requestHeaderVersion')
            ->setPublic()
            ->setReturnType('int')
            ->setParameters([
                new Parameter('apiVersion')->setType('int'),
            ])
            ->setBody(Ast::body(
                Ast::return(
                    Ast::ternary(
                        Ast::call(Ast::this(), 'isFlexibleVersion', Ast::var('apiVersion')),
                        Ast::int(2),
                        Ast::int(1),
                    ),
                ),
            ))
            ->addComment('@return non-negative-int');

        $responseHeaderVersion = Ast::ternary(
            Ast::call(Ast::this(), 'isFlexibleVersion', Ast::var('apiVersion')),
            Ast::int(1),
            Ast::int(0),
        );

        foreach ($keys->keys as $key) {
            if ($key->name !== 'ApiVersions') {
                continue;
            }

            $responseHeaderVersion = Ast::match(
                Ast::this(),
                [
                    Ast::arm(
                        Ast::classConst('self', 'ApiVersions'),
                        Ast::int(0),
                    ),
                    Ast::defaultArm($responseHeaderVersion),
                ],
            );

            break;
        }

        $enum
            ->addMethod('responseHeaderVersion')
            ->setPublic()
            ->setReturnType('int')
            ->setParameters([
                new Parameter('apiVersion')->setType('int'),
            ])
            ->setBody(Ast::body(
                Ast::return(
                    $responseHeaderVersion,
                ),
            ))
            ->addComment('@return non-negative-int');

        $namespace = $this->generatedNs
            ->new()
            ->addUse($this->protocolNs->fqcn('VersionRange'))
            ->addUse($this->generatedNs->fqcn('Listener'))
            ->add($enum);

        $this->fs->write('ApiKey', $namespace);
    }

    /**
     * @param list<non-empty-string> $listeners
     */
    private function generateListener(array $listeners): void
    {
        sort($listeners);

        $enum = new EnumType('Listener')
            ->setType('string')
            ->addComment('@api')
            ->addComment('@generated');

        foreach ($listeners as $listener) {
            $enum->addCase(self::listenerCaseName($listener), $listener);
        }

        $namespace = $this->generatedNs
            ->new()
            ->add($enum);

        $this->fs->write('Listener', $namespace);
    }

    private function generateRequestClass(Schema\Message $request): void
    {
        $namespace = $this->generatedNamespace(
            uses: [
                $this->protocolNs->fqcn('Writer'),
                ...(!$request->flexibleVersions->isNone() ? [$this->protocolNs->fqcn('UnknownTags')] : []),
            ],
        );

        $name = substr($request->name, 0, -7);
        \assert($name !== '');

        $responseType = "{$name}Response";

        $class = $this->generatedClass(
            $request->name,
            [
                $this->generatedNs->fqcn('Request'),
            ],
            [
                "@template-implements Request<{$responseType}>",
            ],
        )
            ->setMethods([
                $this->generateConstructor($namespace, $request->fields, [$request->name], $request->flexibleVersions),
                new Method('apiKey')
                    ->setPublic()
                    ->setReturnType($this->generatedNs->fqcn('ApiKey'))
                    ->setBody(Ast::body(
                        Ast::return(
                            Ast::classConst('ApiKey', $name),
                        ),
                    ))
                    ->addAttribute('Override'),
                new Method('responseType')
                    ->setPublic()
                    ->setReturnType('string')
                    ->setBody(Ast::body(
                        Ast::return(
                            Ast::classConst($responseType, 'class'),
                        ),
                    ))
                    ->addAttribute('Override'),
                $this->generateEncodeMethod($request->fields, $request->validVersions, $request->flexibleVersions),
            ]);

        $namespace->add($class);

        $this->fs->write($request->name, $namespace);
    }

    private function generateResponseClass(Schema\Message $response): void
    {
        $namespace = $this->generatedNamespace(
            uses: [
                $this->protocolNs->fqcn('Reader'),
                ...(!$response->flexibleVersions->isNone() ? [$this->protocolNs->fqcn('UnknownTags')] : []),
            ],
        );

        if ($this->usesUuidFactory($response->fields, $response->validVersions)) {
            $namespace->addUse(Uuid::class);
        }

        $class = $this->generatedClass(
            $response->name,
            [
                $this->generatedNs->fqcn('Response'),
            ],
        )
            ->setMethods([
                $this->generateConstructor($namespace, $response->fields, [$response->name], $response->flexibleVersions),
                $this->generateDecodeMethod($response->fields, $response->validVersions, $response->flexibleVersions, [$response->name], $namespace),
            ]);

        $namespace->add($class);

        $this->fs->write($response->name, $namespace);
    }

    /**
     * @param list<non-empty-string> $namespaceParts
     */
    private function generateInlineStructs(
        Schema\Field $field,
        array $namespaceParts,
        Schema\VersionRange $flex,
    ): void {
        if ($field->fields === []) {
            return;
        }

        $structName = self::structNameFromType($field->type);

        $this->generateStruct(
            $structName,
            $field->fields,
            $field->versions,
            $namespaceParts,
            $flex,
        );

        $childParts = [...$namespaceParts, $structName];
        foreach ($field->fields as $sub) {
            $this->generateInlineStructs($sub, $childParts, $flex);
        }
    }

    private function generateCommonStruct(
        Schema\CommonStruct $struct,
        Schema\Message $message,
        Schema\VersionRange $flex,
    ): void {
        $this->generateStruct(
            $struct->name,
            $struct->fields,
            $struct->versions,
            [$message->name],
            $flex,
        );

        $childParts = [$message->name, $struct->name];
        foreach ($struct->fields as $sub) {
            $this->generateInlineStructs($sub, $childParts, $flex);
        }
    }

    /**
     * @param non-empty-string $structName
     * @param list<Schema\Field> $fields
     * @param list<non-empty-string> $namespaceParts
     */
    private function generateStruct(
        string $structName,
        array $fields,
        Schema\VersionRange $structVersions,
        array $namespaceParts,
        Schema\VersionRange $flex,
    ): void {
        $namespace = $this->generatedNamespace(
            namespaceParts: $namespaceParts,
            uses: [
                $this->protocolNs->fqcn('Writer'),
                $this->protocolNs->fqcn('Reader'),
                $this->protocolNs->fqcn('Encodable'),
                $this->protocolNs->fqcn('Decodable'),
                ...(!$flex->isNone() ? [$this->protocolNs->fqcn('UnknownTags')] : []),
            ],
        );

        if ($this->usesUuidFactory($fields, $structVersions)) {
            $namespace->addUse(Uuid::class);
        }

        $structParts = [...$namespaceParts, $structName];

        $class = $this->generatedClass(
            $structName,
            [
                $this->protocolNs->fqcn('Encodable'),
                $this->protocolNs->fqcn('Decodable'),
            ],
        )
            ->setMethods([
                $this->generateConstructor($namespace, $fields, $structParts, $flex),
                $this->generateEncodeMethod($fields, $structVersions, $flex),
                $this->generateDecodeMethod($fields, $structVersions, $flex, $structParts, $namespace),
            ]);

        $namespace->add($class);

        $this->fs->write(
            implode('/', $namespaceParts) . '/' . $structName,
            $namespace,
        );
    }

    /**
     * @param list<Schema\Field> $fields
     * @param list<non-empty-string> $containerParts
     */
    private function generateConstructor(
        PhpNamespace $namespace,
        array $fields,
        array $containerParts,
        Schema\VersionRange $flex,
    ): Method {
        if ($this->usesUuid($fields)) {
            $namespace->addUse(UuidInterface::class);
        }

        $constructor = new Method('__construct')->setPublic();

        $docLines = [];
        foreach ($fields as $field) {
            $paramName = self::camelCase($field->name);
            $param = $constructor->addPromotedParameter($paramName);

            $nullable = !$field->nullableVersions->isNone();

            $structReference = $this->resolveStructReference($field, $namespace, $containerParts);
            $structTypeReference = $this->resolveStructAbsoluteReference($field, $containerParts);

            $param->setType($this->phpTypeOf($field, $structTypeReference));
            if ($nullable) {
                $param->setNullable();
            }

            $docType = $this->docTypeOf($field, $structReference);
            if ($nullable) {
                $docType = '?' . $docType;
            }

            $docLines[] = self::formatParamLine($paramName, $docType, $field);
        }

        if (!$flex->isNone()) {
            $constructor
                ->addPromotedParameter('unknownTags')
                ->setPublic()
                ->setType($this->protocolNs->fqcn('UnknownTags'))
                ->setDefaultValue(Literal::new('UnknownTags'));

            $docLines[] = '@param UnknownTags $unknownTags Unknown flexible-version tagged fields.';
        }

        if ($docLines !== []) {
            $constructor->setComment(implode("\n", $docLines));
        }

        return $constructor;
    }

    /**
     * @param list<non-empty-string> $namespaceParts
     * @param list<non-empty-string> $uses
     */
    private function generatedNamespace(array $namespaceParts = [], array $uses = []): PhpNamespace
    {
        $namespace = $this->generatedNs->new(...$namespaceParts);

        foreach ($uses as $use) {
            $namespace->addUse($use);
        }

        return $namespace;
    }

    /**
     * @param list<non-empty-string> $implements
     * @param list<string> $comments
     */
    private function generatedClass(string $name, array $implements, array $comments = []): ClassType
    {
        $class = new ClassType($name)
            ->setFinal()
            ->setReadOnly()
            ->setImplements($implements)
            ->addComment('@api')
            ->addComment('@generated');

        foreach ($comments as $comment) {
            $class->addComment($comment);
        }

        return $class;
    }

    /**
     * @param list<Schema\Field> $fields
     * @return list<Schema\Field>
     */
    private static function taggedFields(array $fields): array
    {
        $tagged = array_filter(
            $fields,
            static fn(Schema\Field $field): bool => !$field->taggedVersions->isNone(),
        );

        usort(
            $tagged,
            static fn(Schema\Field $left, Schema\Field $right): int => ($left->tag ?? -1) <=> ($right->tag ?? -1),
        );

        return $tagged;
    }

    /**
     * @param list<Schema\Field> $fields
     */
    private function generateEncodeMethod(
        array $fields,
        Schema\VersionRange $containerVersions,
        Schema\VersionRange $flex,
    ): Method {
        return new Method('encode')
            ->setPublic()
            ->setReturnType('void')
            ->setBody($this->generateEncodeBody($fields, $containerVersions, $flex))
            ->setParameters([
                new Parameter('writer')->setType($this->protocolNs->fqcn('Writer')),
                new Parameter('version')->setType('int'),
            ])
            ->addAttribute('Override');
    }

    /**
     * @param list<Schema\Field> $fields
     * @param list<non-empty-string> $containerParts
     */
    private function generateDecodeMethod(
        array $fields,
        Schema\VersionRange $containerVersions,
        Schema\VersionRange $flex,
        array $containerParts,
        PhpNamespace $namespace,
    ): Method {
        return new Method('decode')
            ->setPublic()
            ->setStatic()
            ->setReturnType('static')
            ->setBody($this->generateDecodeBody($fields, $containerVersions, $flex, $containerParts, $namespace))
            ->setParameters([
                new Parameter('reader')->setType($this->protocolNs->fqcn('Reader')),
                new Parameter('version')->setType('int'),
            ])
            ->addAttribute('Override');
    }

    /**
     * @return non-empty-string
     */
    private static function formatParamLine(
        string $paramName,
        string $docType,
        Schema\Field $field,
    ): string {
        $line = "@param {$docType} \${$paramName}";

        $about = trim($field->about ?? '');
        if ($about !== '') {
            $line .= " {$about}";
        }

        return $line;
    }

    /**
     * @param list<non-empty-string> $containerParts
     * @return ?non-empty-string null when the field is not a struct reference
     */
    private function resolveStructReference(
        Schema\Field $field,
        PhpNamespace $namespace,
        array $containerParts,
    ): ?string {
        $absoluteFqn = $this->resolveStructAbsoluteReference($field, $containerParts);
        if ($absoluteFqn === null) {
            return null;
        }

        return $this->relative($absoluteFqn, $namespace);
    }

    /**
     * @param list<non-empty-string> $containerParts
     * @return ?non-empty-string null when the field is not a struct reference
     */
    private function resolveStructAbsoluteReference(
        Schema\Field $field,
        array $containerParts,
    ): ?string {
        $structName = self::structNameFromType($field->type);

        if (isset(self::SCALARS[$structName]) || isset(self::PREFIXED[$structName])) {
            return null;
        }

        $structParts = $field->fields !== []
            ? [...$containerParts, $structName]
            : [$containerParts[0], $structName]; // @phpstan-ignore offsetAccess.notFound

        return $this->generatedNs->fqcn(...$structParts);
    }

    /**
     * @param ?non-empty-string $structRef
     */
    private function phpTypeOf(Schema\Field $field, ?string $structRef = null): string
    {
        if (str_starts_with($field->type, '[]')) {
            return 'array';
        }

        if ($structRef !== null) {
            return $structRef;
        }

        return self::scalarToPhpType($field->type);
    }

    /**
     * @param ?non-empty-string $structRef
     */
    private function docTypeOf(Schema\Field $field, ?string $structRef): string
    {
        if (str_starts_with($field->type, '[]')) {
            $elementType = substr($field->type, 2);
            $elementDoc = $structRef ?? ($elementType === 'uuid' ? 'UuidInterface' : self::scalarToPhpType($elementType));

            return "list<{$elementDoc}>";
        }

        if ($structRef !== null) {
            return $structRef;
        }

        return $field->type === 'uuid'
            ? 'UuidInterface'
            : self::scalarToPhpType($field->type);
    }

    /**
     * @param list<Schema\Field> $fields
     */
    private function generateEncodeBody(
        array $fields,
        Schema\VersionRange $containerVersions,
        Schema\VersionRange $flex,
    ): string {
        $stmts = [];

        $flexible = self::flexibleCondition($flex);
        $taggedFields = self::taggedFields($fields);

        if ($flexible->kind === Generator\FlexibleKind::Gated) {
            $stmts[] = Ast::assign(Ast::var('flexible'), $flexible->expr);
            $flexible = Generator\FlexibleCondition::gated(Ast::var('flexible'));
        }

        foreach ($fields as $field) {
            if (!$field->taggedVersions->isNone()) {
                continue;
            }

            $fieldStmts = $this->emitField($field, $field->versions, $flex);
            if ($field->versions->covers($containerVersions)) {
                $stmts = [
                    ...$stmts,
                    ...$fieldStmts,
                ];
            } else {
                $stmts[] = Ast::if(
                    self::versionCondition($field->versions, $containerVersions),
                    ...$fieldStmts,
                );
            }
        }

        if (!$flex->isNone()) {
            $taggedStmts = $taggedFields === [] ? [] : [
                Ast::assign(
                    Ast::var('taggedFields'),
                    Ast::new(
                        'UnknownTags',
                        Ast::funcCall('iterator_to_array', Ast::prop(Ast::this(), 'unknownTags')),
                    ),
                ),
            ];

            foreach ($taggedFields as $field) {
                \assert($field->tag !== null);

                $fieldTaggedStmts = [
                    Ast::if(
                        Ast::isset(Ast::arrayGet(Ast::var('taggedFields'), Ast::int($field->tag))),
                        Ast::stmt(
                            Ast::throw(
                                Ast::new(
                                    '\UnexpectedValueException',
                                    Ast::str("Unknown tag '{$field->tag}' collides with known field '{$field->name}'."),
                                ),
                            ),
                        ),
                    ),
                    Ast::assign(Ast::var('tagWriter'), Ast::new('Writer')),
                    ...$this->emitField($field, $field->taggedVersions, $flex, Ast::var('tagWriter')),
                    Ast::assign(
                        Ast::arrayGet(Ast::var('taggedFields'), Ast::int($field->tag)),
                        Ast::castString(Ast::var('tagWriter')),
                    ),
                ];

                if ($field->taggedVersions->covers($flex)) {
                    $taggedStmts = [
                        ...$taggedStmts,
                        ...$fieldTaggedStmts,
                    ];
                } else {
                    $taggedStmts[] = Ast::if(
                        self::versionCondition($field->taggedVersions, $flex),
                        ...$fieldTaggedStmts,
                    );
                }
            }

            $taggedStmts[] = Ast::stmt(
                Ast::call(
                    Ast::var('writer'),
                    'writeTaggedFields',
                    $taggedFields === [] ? Ast::prop(Ast::this(), 'unknownTags') : Ast::var('taggedFields'),
                ),
            );

            $stmts = [
                ...$stmts,
                ...$flexible->gate($taggedStmts),
            ];
        }

        return Ast::body(...$stmts);
    }

    /**
     * @param list<Schema\Field> $fields
     * @param list<non-empty-string> $containerParts
     */
    private function generateDecodeBody(
        array $fields,
        Schema\VersionRange $containerVersions,
        Schema\VersionRange $flex,
        array $containerParts,
        PhpNamespace $namespace,
    ): string {
        $stmts = [];

        $flexible = self::flexibleCondition($flex);
        $taggedFields = self::taggedFields($fields);

        if ($flexible->kind === Generator\FlexibleKind::Gated) {
            $stmts[] = Ast::assign(Ast::var('flexible'), $flexible->expr);
            $flexible = Generator\FlexibleCondition::gated(Ast::var('flexible'));
        }

        $args = [];

        foreach ($fields as $field) {
            $varName = self::camelCase($field->name);
            $args[] = Ast::var($varName);

            if (!$field->taggedVersions->isNone()) {
                continue;
            }

            $structRef = $this->resolveStructReference($field, $namespace, $containerParts);
            $readStmts = $this->emitFieldRead($field, $field->versions, $flex, $varName, $structRef);

            if (!$field->versions->covers($containerVersions)) {
                $readStmts = [
                    Ast::assign(
                        Ast::var($varName),
                        $this->defaultValueFor($field, $structRef, $containerParts, $namespace),
                    ),
                    Ast::if(
                        self::versionCondition($field->versions, $containerVersions),
                        ...$readStmts,
                    ),
                ];
            }

            $stmts = [
                ...$stmts,
                ...$readStmts,
            ];
        }

        if (!$flex->isNone()) {
            $taggedDecodeThen = [
                Ast::assign(
                    Ast::var('unknownTags'),
                    Ast::call(Ast::var('reader'), 'readTaggedFields'),
                ),
            ];

            foreach ($taggedFields as $field) {
                \assert($field->tag !== null);

                $varName = self::camelCase($field->name);
                $structRef = $this->resolveStructReference($field, $namespace, $containerParts);
                $tagValue = Ast::arrayGet(Ast::var('unknownTags'), Ast::int($field->tag));
                $presentTaggedStmts = [
                    Ast::assign(
                        Ast::var('tagReader'),
                        Ast::new('Reader', $tagValue),
                    ),
                    ...$this->emitFieldRead($field, $field->taggedVersions, $flex, $varName, $structRef, Ast::var('tagReader')),
                    Ast::unset(Ast::arrayGet(Ast::var('unknownTags'), Ast::int($field->tag))),
                ];

                $fieldTaggedStmts = [
                    Ast::ifElse(
                        Ast::isset($tagValue),
                        $presentTaggedStmts,
                        [
                            Ast::assign(
                                Ast::var($varName),
                                $this->defaultValueFor($field, $structRef, $containerParts, $namespace),
                            ),
                        ],
                    ),
                ];

                if ($field->taggedVersions->covers($flex)) {
                    $taggedDecodeThen = [
                        ...$taggedDecodeThen,
                        ...$fieldTaggedStmts,
                    ];
                } else {
                    $taggedDecodeThen[] = Ast::ifElse(
                        self::versionCondition($field->taggedVersions, $flex),
                        $fieldTaggedStmts,
                        [
                            Ast::assign(
                                Ast::var($varName),
                                $this->defaultValueFor($field, $structRef, $containerParts, $namespace),
                            ),
                        ],
                    );
                }
            }

            $taggedDecodeElse = [
                Ast::assign(
                    Ast::var('unknownTags'),
                    Ast::new('UnknownTags'),
                ),
                ...array_map(
                    fn(Schema\Field $field): Stmt\Expression => Ast::assign(
                        Ast::var(self::camelCase($field->name)),
                        $this->defaultValueFor(
                            $field,
                            $this->resolveStructReference($field, $namespace, $containerParts),
                            $containerParts,
                            $namespace,
                        ),
                    ),
                    $taggedFields,
                ),
            ];

            $stmts = [
                ...$stmts,
                ...$flexible->branchStmts($taggedDecodeThen, $taggedDecodeElse),
            ];

            $args[] = Ast::var('unknownTags');
        }

        $stmts[] = Ast::return(
            Ast::new('self', ...$args),
        );

        return Ast::body(...$stmts);
    }

    /**
     * @return list<Stmt>
     */
    private function emitField(
        Schema\Field $field,
        Schema\VersionRange $fieldVersions,
        Schema\VersionRange $containerFlex,
        Expr $writer = new Expr\Variable('writer'),
    ): array {
        $mode = self::nullableMode($field->nullableVersions, $fieldVersions);

        return $this->emitValueWrite(
            $field->type,
            Ast::prop(Ast::this(), self::camelCase($field->name)),
            $mode,
            new Generator\EncodeContext(
                writer: $writer,
                flexible: $this->fieldFlexibleCondition($field, $fieldVersions, $containerFlex),
                fieldName: $mode instanceof Generator\VersionGatedNullable ? $field->name : null,
            ),
        );
    }

    /**
     * @return list<Stmt>
     */
    private function emitValueWrite(
        string $type,
        Expr $accessor,
        Generator\NullableMode $mode,
        Generator\EncodeContext $ctx,
    ): array {
        if (str_starts_with($type, '[]')) {
            return $this->emitArrayWrite(substr($type, 2), $accessor, $mode, $ctx);
        }

        if (isset(self::SCALARS[$type])) {
            return [self::emitScalarWrite($type, $accessor, $ctx)];
        }

        if (isset(self::PREFIXED[$type])) {
            return self::emitLengthPrefixedWrite($type, $accessor, $mode, $ctx);
        }

        return self::emitStructWrite($accessor, $mode, $ctx);
    }

    private static function emitScalarWrite(string $type, Expr $accessor, Generator\EncodeContext $ctx): Stmt
    {
        return Ast::stmt(
            Ast::call($ctx->writer, 'write' . ucfirst($type), $accessor),
        );
    }

    /**
     * @return list<Stmt>
     */
    private static function emitLengthPrefixedWrite(
        string $type,
        Expr $accessor,
        Generator\NullableMode $mode,
        Generator\EncodeContext $ctx,
    ): array {
        $base = $type === 'records' ? 'Bytes' : ucfirst($type);

        $write = static fn(Expr $v, string $suffix): Stmt => $ctx->flexible->branch(
            Ast::stmt(Ast::call($ctx->writer, 'writeCompact' . $suffix, $v)),
            Ast::stmt(Ast::call($ctx->writer, 'write' . $suffix, $v)),
        );

        if ($ctx->fieldName === null) {
            return $mode->apply(
                nonNullable: static fn(Expr $v): array => [$write($v, $base)],
                nullable: static fn(Expr $v): array => [$write($v, 'Nullable' . $base)],
                accessor: $accessor,
            );
        }

        $fieldName = $ctx->fieldName;

        return $mode->apply(
            nonNullable: static fn(Expr $v): array => [
                self::nonNullGuard($fieldName, $v),
                $write($v, $base),
            ],
            nullable: static fn(Expr $v): array => [$write($v, 'Nullable' . $base)],
            accessor: $accessor,
        );
    }

    /**
     * @return list<Stmt>
     */
    private function emitArrayWrite(
        string $elementType,
        Expr $accessor,
        Generator\NullableMode $mode,
        Generator\EncodeContext $ctx,
    ): array {
        $elementStmts = $this->emitValueWrite($elementType, Ast::var('item'), Generator\StaticNullableMode::Never, $ctx->forElement());

        $writeLength = static fn(string $suffix, Expr $length): Stmt => $ctx->flexible->branch(
            Ast::stmt(Ast::call($ctx->writer, 'writeCompact' . $suffix, $length)),
            Ast::stmt(Ast::call($ctx->writer, 'write' . $suffix, $length)),
        );

        if ($ctx->fieldName === null) {
            $nonNullable = static fn(Expr $v): array => [
                $writeLength('ArrayLength', Ast::funcCall('\count', $v)),
                Ast::foreach($v, Ast::var('item'), $elementStmts),
            ];
        } else {
            $fieldName = $ctx->fieldName;
            $nonNullable = static fn(Expr $v): array => [
                self::nonNullGuard($fieldName, $v),
                $writeLength('ArrayLength', Ast::funcCall('\count', $v)),
                Ast::foreach($v, Ast::var('item'), $elementStmts),
            ];
        }

        $nullable = static fn(Expr $v): array => [
            $writeLength(
                'NullableArrayLength',
                Ast::ternary(Ast::eq($v, Ast::null()), Ast::null(), Ast::funcCall('\count', $v)),
            ),
            Ast::if(
                Ast::neq($v, Ast::null()),
                Ast::foreach($v, Ast::var('item'), $elementStmts),
            ),
        ];

        return $mode->apply(
            nonNullable: $nonNullable,
            nullable: $nullable,
            accessor: $accessor,
        );
    }

    /**
     * @return list<Stmt>
     */
    private static function emitStructWrite(
        Expr $accessor,
        Generator\NullableMode $mode,
        Generator\EncodeContext $ctx,
    ): array {
        if ($ctx->fieldName === null) {
            $nonNullable = static fn(Expr $v): array => [
                Ast::stmt(Ast::call($v, 'encode', $ctx->writer, Ast::var('version'))),
            ];
        } else {
            $fieldName = $ctx->fieldName;
            $nonNullable = static fn(Expr $v): array => [
                self::nonNullGuard($fieldName, $v),
                Ast::stmt(Ast::call($v, 'encode', $ctx->writer, Ast::var('version'))),
            ];
        }

        $nullable = static fn(Expr $v): array => [
            Ast::stmt(Ast::call($ctx->writer, 'writeBool', Ast::neq($v, Ast::null()))),
            Ast::stmt(Ast::nullsafeCall($v, 'encode', $ctx->writer, Ast::var('version'))),
        ];

        return $mode->apply(
            nonNullable: $nonNullable,
            nullable: $nullable,
            accessor: $accessor,
        );
    }

    /**
     * @param ?non-empty-string $structRef
     * @return list<Stmt>
     */
    private function emitFieldRead(
        Schema\Field $field,
        Schema\VersionRange $fieldVersions,
        Schema\VersionRange $containerFlex,
        string $targetVar,
        ?string $structRef,
        Expr $reader = new Expr\Variable('reader'),
    ): array {
        return $this->emitValueRead(
            $field->type,
            $targetVar,
            self::nullableMode($field->nullableVersions, $fieldVersions),
            $structRef,
            new Generator\DecodeContext(
                reader: $reader,
                flexible: $this->fieldFlexibleCondition($field, $fieldVersions, $containerFlex),
            ),
        );
    }

    /**
     * @param ?non-empty-string $structRef
     * @return list<Stmt>
     */
    private function emitValueRead(
        string $type,
        string $targetVar,
        Generator\NullableMode $mode,
        ?string $structRef,
        Generator\DecodeContext $ctx,
    ): array {
        if (str_starts_with($type, '[]')) {
            return $this->emitArrayRead(substr($type, 2), $targetVar, $mode, $structRef, $ctx);
        }

        if (isset(self::SCALARS[$type])) {
            return [self::emitScalarRead($type, $targetVar, $ctx)];
        }

        if (isset(self::PREFIXED[$type])) {
            return self::emitLengthPrefixedRead($type, $targetVar, $mode, $ctx);
        }

        \assert($structRef !== null);

        return self::emitStructRead($structRef, $targetVar, $mode, $ctx);
    }

    private static function emitScalarRead(string $type, string $targetVar, Generator\DecodeContext $ctx): Stmt
    {
        $method = match ($type) {
            'int8' => 'readInt8',
            'int16' => 'readInt16',
            'int32' => 'readInt32',
            'int64' => 'readInt64',
            'uint16' => 'readUint16',
            'uint32' => 'readUint32',
            'float64' => 'readFloat64',
            'bool' => 'readBool',
            'uuid' => 'readUuid',
            default => throw new \LogicException("Unexpected scalar type: {$type}."),
        };

        return Ast::assign(
            Ast::var($targetVar),
            Ast::call($ctx->reader, $method),
        );
    }

    /**
     * @return list<Stmt>
     */
    private static function emitLengthPrefixedRead(
        string $type,
        string $targetVar,
        Generator\NullableMode $mode,
        Generator\DecodeContext $ctx,
    ): array {
        $base = $type === 'records' ? 'Bytes' : ucfirst($type);

        $read = static fn(string $targetVar, string $suffix): Stmt => $ctx->flexible->branch(
            Ast::assign(Ast::var($targetVar), Ast::call($ctx->reader, 'readCompact' . $suffix)),
            Ast::assign(Ast::var($targetVar), Ast::call($ctx->reader, 'read' . $suffix)),
        );

        return $mode->apply(
            nonNullable: static fn(string $targetVar): array => [$read($targetVar, $base)],
            nullable: static fn(string $targetVar): array => [$read($targetVar, 'Nullable' . $base)],
            accessor: $targetVar,
        );
    }

    /**
     * @param ?non-empty-string $structRef
     * @return list<Stmt>
     */
    private function emitArrayRead(
        string $elementType,
        string $targetVar,
        Generator\NullableMode $mode,
        ?string $structRef,
        Generator\DecodeContext $ctx,
    ): array {
        $countVar = "{$targetVar}Count";
        $indexVar = "{$targetVar}Index";
        $elementRead = $this->emitValueRead($elementType, 'item', Generator\StaticNullableMode::Never, $structRef, $ctx);

        $loopBody = [
            ...$elementRead,
            Ast::push(Ast::var($targetVar), Ast::var('item')),
        ];

        $readLength = static fn(string $suffix): Stmt => $ctx->flexible->branch(
            Ast::assign(Ast::var($countVar), Ast::call($ctx->reader, 'readCompact' . $suffix)),
            Ast::assign(Ast::var($countVar), Ast::call($ctx->reader, 'read' . $suffix)),
        );

        $loop = static fn(): Stmt => Ast::for(
            Ast::assign(Ast::var($indexVar), Ast::int(0)),
            Ast::lt(Ast::var($indexVar), Ast::var($countVar)),
            Ast::preinc(Ast::var($indexVar)),
            $loopBody,
        );

        $nonNullable = static fn(string $targetVar) => [
            $readLength('ArrayLength'),
            Ast::assign(Ast::var($targetVar), Ast::array()),
            $loop(),
        ];

        $nullable = static fn(string $targetVar) => [
            $readLength('NullableArrayLength'),
            Ast::ifElse(
                Ast::eq(Ast::var($countVar), Ast::null()),
                [Ast::assign(Ast::var($targetVar), Ast::null())],
                [
                    Ast::assign(Ast::var($targetVar), Ast::array()),
                    $loop(),
                ],
            ),
        ];

        return $mode->apply(
            nonNullable: $nonNullable,
            nullable: $nullable,
            accessor: $targetVar,
        );
    }

    /**
     * @param non-empty-string $structRef
     * @return list<Stmt>
     */
    private static function emitStructRead(
        string $structRef,
        string $targetVar,
        Generator\NullableMode $mode,
        Generator\DecodeContext $ctx,
    ): array {
        $nonNullable = static fn(string $targetVar): array => [
            Ast::assign(
                Ast::var($targetVar),
                Ast::staticCall($structRef, 'decode', $ctx->reader, Ast::var('version')),
            ),
        ];

        $nullable = static fn(string $targetVar): array => [
            Ast::assign(
                Ast::var($targetVar),
                Ast::ternary(
                    Ast::call($ctx->reader, 'readBool'),
                    Ast::staticCall($structRef, 'decode', $ctx->reader, Ast::var('version')),
                    Ast::null(),
                ),
            ),
        ];

        return $mode->apply(
            nonNullable: $nonNullable,
            nullable: $nullable,
            accessor: $targetVar,
        );
    }

    /**
     * @param list<non-empty-string> $containerParts
     */
    private function defaultValueFor(
        Schema\Field $field,
        ?string $structRef = null,
        array $containerParts = [],
        ?PhpNamespace $namespace = null,
    ): Expr {
        if ($field->default !== null) {
            return self::schemaDefaultValue($field);
        }

        if (!$field->nullableVersions->isNone()) {
            return Ast::null();
        }

        if (str_starts_with($field->type, '[]')) {
            return Ast::array();
        }

        if ($structRef !== null && $field->fields !== []) {
            $args = [];

            /** @var list<non-empty-string> $childContainerParts */
            $childContainerParts = [
                ...$containerParts,
                self::structNameFromType($field->type),
            ];

            foreach ($field->fields as $subField) {
                if (!$subField->taggedVersions->isNone()) {
                    continue;
                }

                $subStructRef = $namespace === null
                    ? null
                    : $this->resolveStructReference($subField, $namespace, $childContainerParts);

                $args[] = $this->defaultValueFor($subField, $subStructRef, $childContainerParts, $namespace);
            }

            \assert($structRef !== '');

            return Ast::new($structRef, ...$args);
        }

        return match ($field->type) {
            'int8',
            'int16',
            'int32',
            'int64',
            'uint16',
            'uint32' => Ast::int(0),
            'float64' => Ast::float(0.0),
            'bool' => Ast::bool(false),
            'string',
            'bytes',
            'records' => Ast::str(''),
            'uuid' => Ast::staticCall(
                'Uuid',
                'fromString',
                Ast::classConst('Uuid', 'NIL'),
            ),
            default => Ast::throw(
                Ast::new(
                    '\LogicException',
                    Ast::str('Required struct is absent at this version.'),
                ),
            ),
        };
    }

    private static function schemaDefaultValue(Schema\Field $field): Expr
    {
        $default = $field->default;

        if ($default === 'null') {
            return Ast::null();
        }

        if ($default === true || $default === 'true') {
            return Ast::bool(true);
        }

        if ($default === false || $default === 'false') {
            return Ast::bool(false);
        }

        if (str_starts_with($field->type, '[]')) {
            throw new \LogicException("Unsupported non-null array default for {$field->name}.");
        }

        if (\is_int($default)) {
            return Ast::int($default);
        }

        if (\is_float($default)) {
            return Ast::float($default);
        }

        if (\is_string($default)) {
            if (preg_match('/^-?(?:0|[1-9][0-9]*)$/', $default) === 1) {
                return Ast::int((int) $default);
            }

            if (preg_match('/^0x[0-9a-fA-F]+$/', $default) === 1) {
                return Ast::int((int) hexdec(substr($default, 2)));
            }

            if (\in_array($field->type, ['string', 'bytes', 'records'], true)) {
                return Ast::str($default);
            }

            if ($field->type === 'uuid') {
                return Ast::staticCall(
                    'Uuid',
                    'fromString',
                    Ast::str($default),
                );
            }
        }

        throw new \LogicException("Unsupported default for {$field->name}: " . var_export($default, true));
    }

    private static function flexibleCondition(Schema\VersionRange $flex): Generator\FlexibleCondition
    {
        return match (true) {
            $flex->isNone() => Generator\FlexibleCondition::never(),
            $flex->min === 0 && $flex->max === null => Generator\FlexibleCondition::always(),
            default => Generator\FlexibleCondition::gated(
                Ast::ge(Ast::var('version'), Ast::int($flex->min ?? 0)),
            ),
        };
    }

    private static function versionCondition(
        Schema\VersionRange $field,
        Schema\VersionRange $container,
    ): Expr {
        $parts = [];

        \assert($field->min !== null);

        if ($container->min === null || $field->min > $container->min) {
            $parts[] = Ast::ge(Ast::var('version'), Ast::int($field->min));
        }

        if ($field->max !== null && ($container->max === null || $field->max < $container->max)) {
            $parts[] = Ast::le(Ast::var('version'), Ast::int($field->max));
        }

        return $parts === [] ? Ast::bool(true) : Ast::and(...$parts);
    }

    private static function versionContainsExpr(Schema\VersionRange $range, Expr $version): Expr
    {
        if ($range->isNone()) {
            return Ast::bool(false);
        }

        $expr = Ast::ge($version, Ast::int($range->min ?? 0));

        if ($range->max === null) {
            return $expr;
        }

        return Ast::and(
            $expr,
            Ast::le($version, Ast::int($range->max)),
        );
    }

    private static function nonNullGuard(string $fieldName, Expr $accessor): Stmt
    {
        return Ast::if(
            Ast::eq($accessor, Ast::null()),
            Ast::stmt(
                Ast::throw(
                    Ast::new(
                        '\UnexpectedValueException',
                        Ast::interpolated(
                            "Field '{$fieldName}' must not be null in version '",
                            Ast::var('version'),
                            "'",
                        ),
                    ),
                ),
            ),
        );
    }

    private static function nullableMode(
        Schema\VersionRange $nullableVersions,
        Schema\VersionRange $fieldVersions,
    ): Generator\NullableMode {
        if ($nullableVersions->isNone() || $nullableVersions->disjoint($fieldVersions)) {
            return Generator\StaticNullableMode::Never;
        }

        if ($nullableVersions->covers($fieldVersions)) {
            return Generator\StaticNullableMode::Always;
        }

        return new Generator\VersionGatedNullable(
            self::versionCondition($nullableVersions, $fieldVersions),
        );
    }

    private function fieldFlexibleCondition(
        Schema\Field $field,
        Schema\VersionRange $fieldVersions,
        Schema\VersionRange $containerFlex,
    ): Generator\FlexibleCondition {
        $flexibleVersions = $field->flexibleVersions ?? $containerFlex;

        if ($flexibleVersions->isNone() || $flexibleVersions->disjoint($fieldVersions)) {
            return Generator\FlexibleCondition::never();
        }

        if ($flexibleVersions->covers($fieldVersions)) {
            return Generator\FlexibleCondition::always();
        }

        if ($field->flexibleVersions === null) {
            return Generator\FlexibleCondition::gated(Ast::var('flexible'));
        }

        return Generator\FlexibleCondition::gated(self::versionCondition($flexibleVersions, $fieldVersions));
    }

    /**
     * @param non-empty-string $absoluteFqn
     * @return non-empty-string
     */
    private function relative(string $absoluteFqn, PhpNamespace $namespace): string
    {
        $current = $namespace->getName();

        if ($current !== '' && str_starts_with($absoluteFqn, $current . '\\')) {
            /** @var non-empty-string */
            return substr($absoluteFqn, \strlen($current) + 1);
        }

        $namespace->addUse($absoluteFqn);

        $slash = strrpos($absoluteFqn, '\\');

        /** @var non-empty-string */
        return $slash === false ? $absoluteFqn : substr($absoluteFqn, $slash + 1);
    }

    private static function scalarToPhpType(string $type): string
    {
        if (isset(self::PREFIXED[$type])) {
            return 'string';
        }

        return match ($type) {
            'int8',
            'int16',
            'int32',
            'int64',
            'uint16',
            'uint32' => 'int',
            'float64' => 'float',
            'bool' => 'bool',
            'uuid' => UuidInterface::class,
            default => throw new \LogicException("Not a scalar primitive: {$type}."),
        };
    }

    /**
     * @param list<Schema\Field> $fields
     */
    private function usesUuid(array $fields): bool
    {
        return array_any(
            $fields,
            static fn(Schema\Field $field) => \in_array($field->type, ['uuid', '[]uuid'], true),
        );
    }

    /**
     * @param list<Schema\Field> $fields
     */
    private function usesUuidFactory(array $fields, Schema\VersionRange $containerVersions): bool
    {
        foreach ($fields as $field) {
            if ($field->type !== 'uuid') {
                continue;
            }

            if ($field->default !== null || !$field->versions->covers($containerVersions)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param non-empty-string $listener
     * @return non-empty-string
     */
    private static function listenerCaseName(string $listener): string
    {
        $listener = str_replace(['-', '_'], ' ', $listener);

        /** @var non-empty-string */
        return str_replace(' ', '', ucwords($listener));
    }

    /**
     * @return non-empty-string
     */
    private static function structNameFromType(string $type): string
    {
        if (str_starts_with($type, '[]')) {
            $type = substr($type, 2);
        }

        /** @var non-empty-string */
        return $type;
    }

    /**
     * @param non-empty-string $name
     * @return non-empty-string
     */
    private static function camelCase(string $name): string
    {
        return lcfirst($name);
    }

    /**
     * @param non-empty-string $json
     * @return array<string, mixed>
     * @throws SyntaxError
     */
    private static function json5decode(string $json): array
    {
        $decoded = json5_decode($json, true);
        if (!\is_array($decoded)) {
            throw new \RuntimeException("JSON input is not an array: {$json}.");
        }

        /** @var array<string, mixed> */
        return $decoded;
    }
}

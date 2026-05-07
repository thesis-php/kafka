<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Generator;

use Nette\PhpGenerator\PhpNamespace;

/**
 * @internal
 */
final readonly class NamespaceGenerator
{
    /**
     * @param non-empty-string $namespace
     */
    public function __construct(
        private string $namespace,
    ) {}

    /**
     * @param non-empty-string ...$names
     */
    public function new(string ...$names): PhpNamespace
    {
        $ns = $this->namespace;

        if ($names !== []) {
            $ns .= '\\' . implode('\\', $names);
        }

        return new PhpNamespace($ns);
    }

    /**
     * @param non-empty-string ...$names
     * @return non-empty-string
     */
    public function fqcn(string ...$names): string
    {
        $class = implode('\\', $names);

        return "{$this->namespace}\\{$class}";
    }
}

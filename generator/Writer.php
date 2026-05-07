<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator;

use Nette\PhpGenerator\PhpNamespace;

/**
 * @internal
 */
interface Writer
{
    /**
     * @param non-empty-string $file
     */
    public function write(string $file, PhpNamespace $namespace): void;
}

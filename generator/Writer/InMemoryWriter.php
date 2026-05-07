<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Writer;

use Nette\PhpGenerator;
use Thesis\Kafka\Generator\Printer;
use Thesis\Kafka\Generator\Writer;

/**
 * @internal
 */
final class InMemoryWriter implements Writer
{
    /** @var array<non-empty-string, string> */
    private array $files = [];

    public function __construct(
        private readonly PhpGenerator\Printer $printer = new Printer(),
    ) {}

    #[\Override]
    public function write(string $file, PhpGenerator\PhpNamespace $namespace): void
    {
        $phpFile = new PhpGenerator\PhpFile()
            ->setStrictTypes()
            ->add($namespace);

        $this->files["{$file}.php"] = $this->printer->printFile($phpFile);
    }

    /**
     * @param non-empty-string $file
     */
    public function get(string $file): ?string
    {
        return $this->files[$file] ?? null;
    }
}

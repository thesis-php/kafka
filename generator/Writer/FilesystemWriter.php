<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\Writer;

use Nette\PhpGenerator;
use Thesis\Kafka\Generator\Printer;
use Thesis\Kafka\Generator\Writer;

/**
 * @internal
 */
final readonly class FilesystemWriter implements Writer
{
    /**
     * @param non-empty-string $dir
     */
    public function __construct(
        private string $dir,
        private PhpGenerator\Printer $printer = new Printer(),
    ) {}

    #[\Override]
    public function write(string $file, PhpGenerator\PhpNamespace $namespace): void
    {
        $path = "{$this->dir}/{$file}.php";

        $dir = \dirname($path);
        if (!is_dir($dir)) {
            mkdir($dir, recursive: true);
        }

        $file = new PhpGenerator\PhpFile()
            ->setStrictTypes()
            ->add($namespace);

        $contents = $this->printer
            ->setTypeResolving(true)
            ->printFile($file);

        file_put_contents($path, $contents);
    }
}

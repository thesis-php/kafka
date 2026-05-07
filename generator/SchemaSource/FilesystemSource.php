<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator\SchemaSource;

use Thesis\Kafka\Generator\SchemaSource;

/**
 * @internal
 */
final readonly class FilesystemSource implements SchemaSource
{
    /**
     * @param non-empty-list<non-empty-string> $paths
     */
    public function __construct(
        private array $paths,
    ) {}

    #[\Override]
    public function schemas(): iterable
    {
        foreach ($this->paths as $path) {
            $it = new \RecursiveIteratorIterator(
                new \RecursiveDirectoryIterator(
                    $path,
                    \RecursiveDirectoryIterator::SKIP_DOTS,
                ),
            );

            /** @var \SplFileInfo $file */
            foreach ($it as $file) {
                if (!$file->isFile() || $file->getExtension() !== 'json') {
                    continue;
                }

                $contents = file_get_contents($file->getPathname());
                if ($contents === false || $contents === '') {
                    throw new \RuntimeException("Could not read file: {$file->getPathname()}.");
                }

                yield $contents;
            }
        }
    }
}

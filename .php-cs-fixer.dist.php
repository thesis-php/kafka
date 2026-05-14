<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;
use PhpCsFixer\Runner\Parallel\ParallelConfigFactory;
use PHPyh\CodingStandard\PhpCsFixerCodingStandard;

$config = new Config()
    ->setFinder(
        Finder::create()
            ->in(__DIR__ . '/examples')
            ->in(__DIR__ . '/src')
            ->in(__DIR__ . '/generator')
            ->in(__DIR__ . '/tests')
            ->append([
                __FILE__,
            ]),
    )
    ->setParallelConfig(ParallelConfigFactory::detect())
    ->setCacheFile(__DIR__ . '/var/' . basename(__FILE__) . '.cache');

new PhpCsFixerCodingStandard()->applyTo($config, [
    'numeric_literal_separator' => false,
    'blank_line_before_statement' => false,
    'phpdoc_annotation_without_dot' => false,
    'fully_qualified_strict_types' => false,
]);

return $config;

<?php

declare(strict_types=1);

namespace Thesis\Kafka\Generator;

require_once __DIR__ . '/../vendor/autoload.php';

$generator = new Generator(
    fs: new Writer\FilesystemWriter(
        dir: __DIR__ . '/../src/Protocol/Generated',
    ),
    protocolNs: new Generator\NamespaceGenerator('Thesis\Kafka\Protocol'),
    generatedNs: new Generator\NamespaceGenerator('Thesis\Kafka\Protocol\Generated'),
);

$generator->generate(new SchemaSource\FilesystemSource([
    __DIR__ . '/../schemas/messages',
]));

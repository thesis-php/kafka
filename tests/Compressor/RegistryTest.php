<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Testo\Assert;
use Testo\Expect;
use Testo\Test;
use Thesis\Kafka\Exception\UnsupportedCodec;

#[Test]
final readonly class RegistryTest
{
    public function none(): void
    {
        $registry = new Registry();

        Assert::same(None::Compressor, $registry->for(Codec::None));
    }

    public function unsupportedCodec(): void
    {
        $registry = new Registry();

        Expect::exception(new UnsupportedCodec(Codec::Gzip))
            ->fromMethod(Registry::class, 'for');

        $registry->for(Codec::Gzip);
    }

    public function codecFound(): void
    {
        $registry = new Registry([
            new Gzip(),
        ]);

        Assert::instanceOf($registry->for(Codec::Gzip), Gzip::class);
    }

    public function addCodec(): void
    {
        $registry = new Registry();
        $registry = $registry->add(new Gzip());

        Assert::count($registry, 1);
        Assert::instanceOf($registry->for(Codec::Gzip), Gzip::class);
    }
}

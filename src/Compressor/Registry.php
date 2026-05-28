<?php

declare(strict_types=1);

namespace Thesis\Kafka\Compressor;

use Thesis\Kafka\Compressor;
use Thesis\Kafka\Exception\UnsupportedCodec;

/**
 * @api
 * @template-implements \IteratorAggregate<array-key, Compressor>
 */
final readonly class Registry implements
    \IteratorAggregate,
    \Countable
{
    /** @var \SplObjectStorage<Codec, Compressor> */
    private \SplObjectStorage $codecs;

    /**
     * @param list<Compressor> $compressors
     */
    public function __construct(
        private array $compressors = [],
    ) {
        $this->codecs = new \SplObjectStorage();

        foreach ($compressors as $compressor) {
            $this->codecs->attach($compressor->codec(), $compressor);
        }
    }

    /**
     * @no-named-arguments
     */
    public function add(Compressor ...$compressors): self
    {
        return new self([
            ...$this->compressors,
            ...$compressors,
        ]);
    }

    /**
     * @throws UnsupportedCodec if a batch uses a codec we have no
     *                           decompressor for
     */
    public function for(Codec $codec): Compressor
    {
        if ($codec === Codec::None) {
            return Compressor\None::Compressor;
        }

        return $this->codecs[$codec]
            ?? throw new UnsupportedCodec($codec);
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        yield from $this->compressors;
    }

    #[\Override]
    public function count(): int
    {
        return \count($this->compressors);
    }
}

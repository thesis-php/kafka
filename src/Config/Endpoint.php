<?php

declare(strict_types=1);

namespace Thesis\Kafka\Config;

/**
 * @api
 */
final readonly class Endpoint implements \Stringable
{
    /**
     * @param non-empty-string $host
     * @param int<1, 65535> $port
     */
    public function __construct(
        public string $host,
        public int $port,
    ) {}

    /**
     * @param Endpoint|non-empty-string $endpoint
     */
    public static function parse(self|string $endpoint): self
    {
        if (\is_string($endpoint)) {
            $endpoint = self::fromString($endpoint);
        }

        return $endpoint;
    }

    /**
     * @param non-empty-string $url
     */
    public static function fromString(string $url): self
    {
        $parsed = parse_url($url);

        if (!\is_array($parsed)) {
            throw new \UnexpectedValueException("Invalid URL: {$url}");
        }

        $host = $parsed['host'] ?? '';
        if ($host === '') {
            throw new \UnexpectedValueException("Invalid host in URL: {$host}");
        }

        $port = $parsed['port'] ?? 0;
        if ($port < 1) {
            throw new \UnexpectedValueException("Invalid port in URL: {$port}");
        }

        return new self($host, $port);
    }

    public function matches(self $endpoint): bool
    {
        return $this->host === $endpoint->host;
    }

    /**
     * @return non-empty-string
     */
    #[\Override]
    public function __toString(): string
    {
        return "{$this->host}:{$this->port}";
    }
}

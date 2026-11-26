<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Exception\UnexpectedEof;
use Thesis\Kafka\Exception\UnexpectedValue;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\PeerFault;
use Thesis\Time\TimeSpan;

/**
 * The peer answers ApiVersions correctly. The bad varint is the first field
 * of the FindCoordinator response: its tagged fields of the header. So the
 * error comes from the varint and not from a fallback decode of ApiVersions.
 */
final readonly class MalformedVarintTest
{
    #[Test]
    public function responseCutInsideVarintFailsWithUnexpectedEof(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::CutVarint);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);

            Assert::same(self::failure($client), [UnexpectedEof::class, 'Varint at offset 4 ends after 1 byte(s)']);
            Assert::same($peer->accepted(), 1);
        });
    }

    #[Test]
    public function varintOfElevenBytesFailsWithUnexpectedValue(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::LongVarint);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);

            Assert::same(self::failure($client), [UnexpectedValue::class, 'Varint at offset 4 does not fit in 64 bits']);
            Assert::same($peer->accepted(), 1);
        });
    }

    /**
     * @return array{class-string, string}|null class and message of the error
     */
    private static function failure(Client $client): ?array
    {
        try {
            $client->request(new Generated\FindCoordinatorRequest(
                key: 'thesis-group-malformed-varint',
                keyType: 0,
                coordinatorKeys: ['thesis-group-malformed-varint'],
            ));
        } catch (\Throwable $e) {
            return [$e::class, $e->getMessage()];
        }

        return null;
    }
}

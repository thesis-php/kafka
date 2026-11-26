<?php

declare(strict_types=1);

namespace Thesis\Kafka;

use Testo\Assert;
use Testo\Test;
use Thesis\Kafka\Exception\ProtocolError;
use Thesis\Kafka\Protocol\Error;
use Thesis\Kafka\Protocol\ErrorCode;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Testing\Kafka;
use Thesis\Kafka\Testing\PeerFault;
use Thesis\Time\TimeSpan;

final readonly class UnknownErrorCodeTest
{
    #[Test]
    public function unknownErrorCodeIsUnknownServerError(): void
    {
        Kafka::connect()->timeout(TimeSpan::fromSeconds(10))->test(static function (Kafka $kafka): void {
            $peer = $kafka->brokenPeer(PeerFault::UnknownErrorCode);
            $client = $kafka->createClient(seeds: [$peer->endpoint]);

            $failure = null;

            try {
                $client->request(new Generated\FindCoordinatorRequest(
                    key: 'thesis-group-unknown-error-code',
                    keyType: 0,
                    coordinatorKeys: ['thesis-group-unknown-error-code'],
                ));
            } catch (\Throwable $e) {
                $failure = $e;
            }

            Assert::instanceOf($failure, ProtocolError::class);
            Assert::same(
                array_map(static fn(Error $error) => $error->code(), $failure->errors->errors),
                [ErrorCode::UnknownServerError],
            );
            Assert::same($peer->accepted(), 1);
        });
    }
}

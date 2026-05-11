<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Scram;

use Thesis\Kafka\SASL\Authenticator;
use Thesis\Kafka\SASL\Session;

/**
 * @internal
 */
abstract readonly class ScramAuthenticator implements Authenticator
{
    private const int DEFAULT_MIN_ITERATIONS = 4096;
    private const int DEFAULT_NONCE_LENGTH = 20;

    /**
     * @param non-empty-string $username
     * @param non-empty-string $password
     * @param ?string $nonce optional fixed nonce (testing only)
     * @param bool $isToken if true, appends ",tokenauth=true" — delegation tokens (KIP-48)
     * @param int<4096, max> $minIterations RFC 5802 recommends at least 4096
     * @param int<20, max> $nonceLength 20 bytes (160 bits) is more than enough for uniqueness
     */
    final public function __construct(
        private string $username,
        private string $password,
        private string $zid = '',
        private ?string $nonce = null,
        private bool $isToken = false,
        private int $minIterations = self::DEFAULT_MIN_ITERATIONS,
        private int $nonceLength = self::DEFAULT_NONCE_LENGTH,
    ) {}

    abstract protected function algorithm(): HashAlgorithm;

    #[\Override]
    final public function mechanism(): string
    {
        return match ($this->algorithm()) {
            HashAlgorithm::SHA256 => 'SCRAM-SHA-256',
            HashAlgorithm::SHA512 => 'SCRAM-SHA-512',
        };
    }

    #[\Override]
    final public function authenticate(): Session
    {
        /** @var non-empty-string $nonce */
        $nonce = base64_encode($this->nonce ?? random_bytes($this->nonceLength));

        // SCRAM uses "=" and "," as delimiters, so any of those characters
        // inside the username must be quoted.
        $user = strtr($this->username, ['=' => '=3D', ',' => '=2C']);

        // client-first-message-bare is the part of the first message that
        // the server will echo verbatim into the final message.
        $clientFirstBare = "n={$user},r={$nonce}";
        if ($this->isToken) {
            $clientFirstBare .= ',tokenauth=true';
        }

        // GS2 header: channel binding flag + optional authzid.
        $gs2Header = 'n,';
        if ($this->zid !== '') {
            $gs2Header .= 'a=' . strtr($this->zid, ['=' => '=3D', ',' => '=2C']);
        }

        $gs2Header .= ',';

        return new ScramSession(
            algorithm: $this->algorithm(),
            password: $this->password,
            clientNonce: $nonce,
            clientFirstBare: $clientFirstBare,
            clientFirstMessage: $gs2Header . $clientFirstBare,
            minIterations: $this->minIterations,
        );
    }
}

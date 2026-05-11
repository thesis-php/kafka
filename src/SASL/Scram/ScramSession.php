<?php

declare(strict_types=1);

namespace Thesis\Kafka\SASL\Scram;

use Thesis\Kafka\Exception\SaslAuthenticationFailed;
use Thesis\Kafka\SASL;

/**
 * @internal
 */
final class ScramSession implements SASL\Session
{
    /**
     * Empty until step 1 has run.
     */
    private string $expectedServerSignature = '';

    /**
     * @param non-empty-string $password
     * @param non-empty-string $clientNonce
     * @param non-empty-string $clientFirstBare
     * @param non-empty-string $clientFirstMessage
     * @param positive-int $minIterations
     */
    public function __construct(
        private readonly HashAlgorithm $algorithm,
        private readonly string $password,
        private readonly string $clientNonce,
        private readonly string $clientFirstBare,
        private readonly string $clientFirstMessage,
        private readonly int $minIterations,
    ) {}

    #[\Override]
    public function start(): SASL\Exchange
    {
        // First wire message: send the GS2 header + client-first-bare.
        // The broker replies with server-first-message containing the
        // combined nonce, salt, and iteration count. State::Continue
        // because we'll be back in challenge() to process that reply.
        return SASL\Exchange::continue($this->clientFirstMessage);
    }

    #[\Override]
    public function challenge(int $step, string $bytes): SASL\Exchange
    {
        return match ($step) {
            1 => $this->processServerFirst($bytes),
            2 => $this->processServerFinal($bytes),
            default => throw new SaslAuthenticationFailed(
                "SCRAM exchange should be done at step 2, got step {$step}",
            ),
        };
    }

    /**
     * server-first format:
     *     r=<combined-nonce>,s=<base64-salt>,i=<iterations>[,extensions]
     *
     * @throws SaslAuthenticationFailed
     */
    private function processServerFirst(string $serverFirst): SASL\Exchange
    {
        $kvs = self::parseAttributes($serverFirst);

        $combinedNonce = $kvs['r'] ?? throw new SaslAuthenticationFailed(
            'SCRAM server-first-message missing r= (nonce)',
        );

        // The server appends its own nonce to ours. If the prefix doesn't
        // match, we're either talking to the wrong session or to an
        // attacker — abort before deriving any keys.
        if (!str_starts_with($combinedNonce, $this->clientNonce)) {
            throw new SaslAuthenticationFailed(
                'SCRAM server nonce does not start with client nonce — possible replay attack',
            );
        }

        $saltB64 = $kvs['s'] ?? throw new SaslAuthenticationFailed(
            'SCRAM server-first-message missing s= (salt)',
        );

        $salt = base64_decode($saltB64, true);
        if ($salt === false) {
            throw new SaslAuthenticationFailed('SCRAM server salt is not valid base64');
        }

        $itersRaw = $kvs['i'] ?? throw new SaslAuthenticationFailed(
            'SCRAM server-first-message missing i= (iterations)',
        );

        // ctype_digit guarantees a non-negative integer literal without
        // signs, decimals, or whitespace. (int) on "12abc" silently
        // returns 12, which would be bad here.
        if (!ctype_digit($itersRaw)) {
            throw new SaslAuthenticationFailed("SCRAM iterations is not a non-negative integer: {$itersRaw}");
        }

        $iterations = (int) $itersRaw;
        if ($iterations < $this->minIterations) {
            throw new SaslAuthenticationFailed(
                "SCRAM iterations {$iterations} below minimum {$this->minIterations}",
            );
        }

        $password = hash_pbkdf2(
            $this->algorithm->value,
            $this->password,
            $salt,
            $iterations,
            binary: true,
        );

        $clientKey = hash_hmac($this->algorithm->value, 'Client Key', $password, true);
        $storedKey = hash($this->algorithm->value, $clientKey, true);

        $clientFinalWithoutProof = "c=biws,r={$combinedNonce}";

        // authMessage is the concatenation that both sides hash to prove
        // they computed the same thing. The server runs this same
        // construction to verify our proof.
        $authMessage = "{$this->clientFirstBare},{$serverFirst},{$clientFinalWithoutProof}";

        $clientSignature = hash_hmac($this->algorithm->value, $authMessage, $storedKey, true);
        $clientProof = $clientKey ^ $clientSignature;

        $serverKey = hash_hmac($this->algorithm->value, 'Server Key', $password, true);
        $this->expectedServerSignature = base64_encode(
            hash_hmac($this->algorithm->value, $authMessage, $serverKey, true),
        );

        $clientFinal = $clientFinalWithoutProof . ',p=' . base64_encode($clientProof);

        return SASL\Exchange::continue($clientFinal);
    }

    /**
     * server-final format:
     *     v=<base64-server-signature> on success
     *     e=<sasl-error-string> on auth failure
     *
     * @throws SaslAuthenticationFailed
     */
    private function processServerFinal(string $serverFinal): SASL\Exchange
    {
        $kvs = self::parseAttributes($serverFinal);

        if (isset($kvs['e'])) {
            throw new SaslAuthenticationFailed("SCRAM server rejected authentication: {$kvs['e']}");
        }

        $signature = $kvs['v'] ?? throw new SaslAuthenticationFailed(
            'SCRAM server-final-message missing v= (server signature)',
        );

        if (!hash_equals($this->expectedServerSignature, $signature)) {
            throw new SaslAuthenticationFailed(
                'SCRAM server signature mismatch: the broker did not prove knowledge of the password',
            );
        }

        return SASL\Exchange::done();
    }

    /**
     * @return array<string, string>
     */
    private static function parseAttributes(string $message): array
    {
        $kvs = [];

        foreach (explode(',', $message) as $kv) {
            if ($kv === '') {
                continue;
            }

            $eq = strpos($kv, '=');
            if ($eq === false) {
                continue;
            }

            $key = substr($kv, 0, $eq);
            $value = substr($kv, $eq + 1);

            $kvs[$key] = $value;
        }

        return $kvs;
    }
}

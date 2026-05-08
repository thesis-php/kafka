<?php

declare(strict_types=1);

namespace Thesis\Kafka\Runtime\Internal;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\NullCancellation;
use Amp\Pipeline;
use Amp\Socket\Socket;
use Revolt\EventLoop;
use Thesis\Kafka\Protocol\Generated;
use Thesis\Kafka\Protocol\Reader;
use Thesis\Kafka\Runtime\Internal\Connection\CorrelationIds;

/**
 * @internal
 */
final class Connection
{
    private const int SEND_BUFFER_SIZE = 10;

    private readonly CorrelationIds $correlation;

    /** @var Pipeline\Queue<Connection\PendingRequest<*>> */
    private readonly Pipeline\Queue $send;

    /** @var array<non-negative-int, Connection\PendingRequest<*>> */
    private array $pending = [];

    private bool $running = false;

    public function __construct(
        private readonly Socket $socket,
        int $sendBufferSize = self::SEND_BUFFER_SIZE,
    ) {
        $this->correlation = new CorrelationIds();
        $this->send = new Pipeline\Queue(bufferSize: $sendBufferSize);
    }

    /**
     * @template R of Generated\Response
     * @param Call<R> $call
     * @return Future<R>
     */
    public function call(
        Call $call,
        Cancellation $cancellation = new NullCancellation(),
    ): Future {
        /** @var DeferredFuture<R> $deferred */
        $deferred = new DeferredFuture();

        $id = $this->correlation->id;

        $request = new Connection\PendingRequest(
            $call,
            $deferred,
            $id,
        );

        $pending = &$this->pending;
        $pending[$id] = $request;

        $cancellation->subscribe(static function () use (
            &$pending,
            $id,
        ): void {
            unset($pending[$id]);
        });

        if (!$this->running) {
            $this->run();
        }

        $this->send->push($request);

        return $deferred->getFuture();
    }

    public function close(): void
    {
        if ($this->running) {
            $this->running = false;
            $this->socket->close();
            $this->send->complete();
        }
    }

    private function run(): void
    {
        $pending = &$this->pending;
        $send = $this->send;
        $socket = $this->socket;

        EventLoop::queue(static function () use (
            &$pending,
            $socket,
        ): void {
            $buffer = new Connection\ReadBuffer();

            while (($chunk = $socket->read()) !== null) {
                foreach ($buffer->push($chunk) as $frame) {
                    $reader = new Reader($frame);

                    $correlationId = $reader->readInt32();
                    if (!isset($pending[$correlationId])) {
                        continue;
                    }

                    $request = $pending[$correlationId];
                    unset($pending[$correlationId]);

                    try {
                        $headerVersion = $request->call->request
                            ->apiKey()
                            ->responseHeaderVersion($request->call->version);

                        if ($headerVersion >= 1) {
                            $reader->readTaggedFields();
                        }

                        $responseType = $request->call->request->responseType();
                        $response = $responseType::decode($reader, $request->call->version);
                        $request->deferred->complete($response);
                    } catch (\Throwable $e) {
                        $request->deferred->error($e);
                    }
                }
            }
        });

        EventLoop::queue(static function () use (
            $send,
            $socket,
        ): void {
            $buffer = new Connection\WriteBuffer($socket);

            $iterator = $send->iterate();

            /** @var Connection\PendingRequest<*> $request */
            foreach ($iterator as $request) {
                $buffer->write($request);
            }
        });

        $this->running = true;
    }
}

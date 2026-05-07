<?php

declare(strict_types=1);

namespace Thesis\Kafka\Protocol\Generated;

use Ramsey\Uuid\UuidInterface;
use Thesis\Kafka\Protocol\UnknownTags;
use Thesis\Kafka\Protocol\Writer;

/**
 * @api
 * @generated
 * @template-implements Request<ControllerRegistrationResponse>
 */
final readonly class ControllerRegistrationRequest implements Request
{
    /**
     * @param int $controllerId The ID of the controller to register.
     * @param UuidInterface $incarnationId The controller incarnation ID, which is unique to each process run.
     * @param bool $zkMigrationReady Set if the required configurations for ZK migration are present.
     * @param list<ControllerRegistrationRequest\Listener> $listeners The listeners of this controller.
     * @param list<ControllerRegistrationRequest\Feature> $features The features on this controller.
     * @param UnknownTags $unknownTags Unknown flexible-version tagged fields.
     */
    public function __construct(
        public int $controllerId,
        public UuidInterface $incarnationId,
        public bool $zkMigrationReady,
        public array $listeners,
        public array $features,
        public UnknownTags $unknownTags = new UnknownTags(),
    ) {}

    #[\Override]
    public function apiKey(): ApiKey
    {
        return ApiKey::ControllerRegistration;
    }

    #[\Override]
    public function responseType(): string
    {
        return ControllerRegistrationResponse::class;
    }

    #[\Override]
    public function encode(Writer $writer, int $version): void
    {
        $writer->writeInt32($this->controllerId);
        $writer->writeUuid($this->incarnationId);
        $writer->writeBool($this->zkMigrationReady);
        $writer->writeCompactArrayLength(\count($this->listeners));
        foreach ($this->listeners as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeCompactArrayLength(\count($this->features));
        foreach ($this->features as $item) {
            $item->encode($writer, $version);
        }
        $writer->writeTaggedFields($this->unknownTags);
    }
}

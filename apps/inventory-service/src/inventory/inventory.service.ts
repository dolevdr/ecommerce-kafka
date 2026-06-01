import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InventoryReservedEvent, OrderCreatedEvent, TOPICS } from '@ecommerce-kafka/shared';
import { PrismaService } from '../prisma/prisma.service';
import { v4 as uuid } from 'uuid';

@Injectable()
export class InventoryService implements OnModuleInit {
  private readonly logger = new Logger(InventoryService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly prisma: PrismaService,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer connected');
  }

  async reserveStock(event: OrderCreatedEvent) {
    this.logger.log(
      `Received Order ${event.orderId} — reserving ${event.quantity} of ${event.productId}`,
    );

    const existing = await this.prisma.inventoryReservation.findUnique({
      where: { orderId: event.orderId },
    });

    if (existing) {
      this.logger.log(`Order ${event.orderId} already processed — skipping`);
      return existing;
    }

    const reservation = await this.prisma.inventoryReservation.create({
      data: {
        orderId: event.orderId,
        productId: event.productId,
        quantity: event.quantity,
        status: 'RESERVED',
      },
    });

    const reservedEvent: InventoryReservedEvent = {
      createdAt: reservation.createdAt.toISOString(),
      customerId: event.customerId,
      eventId: uuid(),
      orderId: reservation.orderId,
      productId: reservation.productId,
      quantity: reservation.quantity,
      reservationId: reservation.id,
    };

    this.kafkaClient.emit(TOPICS.INVENTORY_RESERVED, {
      key: reservation.orderId,
      value: JSON.stringify(reservedEvent),
    });

    this.logger.log(
      `Stock reserved for Order ${event.orderId} (reservation #${reservation.id}) — event published`,
    );
    return reservation;
  }
}

import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  InventoryReservationFailedEvent,
  InventoryReservedEvent,
  OrderCreatedEvent,
  PaymentFailedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
import { PrismaService } from '../prisma/prisma.service';
import { ReservationStatus } from '@prisma/client';
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

    if (event.quantity > 5) {
      const reservation = await this.prisma.inventoryReservation.create({
        data: {
          orderId: event.orderId,
          productId: event.productId,
          quantity: event.quantity,
          status: 'FAILED',
        },
      });

      const failedEvent: InventoryReservationFailedEvent = {
        createdAt: reservation.createdAt.toISOString(),
        eventId: uuid(),
        orderId: event.orderId,
        productId: event.productId,
        quantity: event.quantity,
        reason: `Insufficient stock: requested ${event.quantity}, max allowed is 5`,
      };

      this.kafkaClient.emit(TOPICS.INVENTORY_RESERVATION_FAILED, {
        key: event.orderId,
        value: JSON.stringify(failedEvent),
      });

      this.logger.warn(
        `Stock reservation FAILED for Order ${event.orderId} — ${failedEvent.reason}`,
      );
      return reservation;
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

  async releaseReservation(event: PaymentFailedEvent) {
    this.logger.log(
      `Payment failed for Order ${event.orderId} — releasing reserved stock`,
    );

    const reservation = await this.prisma.inventoryReservation.findUnique({
      where: { orderId: event.orderId },
    });

    if (!reservation) {
      this.logger.warn(`No reservation found for Order ${event.orderId} — skipping`);
      return;
    }

    if (reservation.status === ReservationStatus.RELEASED) {
      this.logger.log(`Reservation for Order ${event.orderId} already released — skipping`);
      return reservation;
    }

    const updated = await this.prisma.inventoryReservation.update({
      where: { orderId: event.orderId },
      data: { status: ReservationStatus.RELEASED },
    });

    this.logger.log(
      `Stock released for Order ${event.orderId} (reservation #${updated.id})`,
    );
    return updated;
  }
}

import { OrderCreatedEvent } from '@ecommerce-kafka/shared';
import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class InventoryService {
  private readonly logger = new Logger(InventoryService.name);

  constructor(private readonly prisma: PrismaService) {}

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

    this.logger.log(
      `Stock reserved for Order ${event.orderId} (reservation #${reservation.id})`,
    );
    return reservation;
  }
}

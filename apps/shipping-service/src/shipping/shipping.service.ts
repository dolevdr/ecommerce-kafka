import {
  PaymentCompletedEvent,
  ShipmentDeliveredEvent,
  ShipmentDispatchedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { ShipmentStatus } from '@prisma/client';
import { v4 as uuid } from 'uuid';
import { PrismaService } from '../prisma/prisma.service';

const CARRIERS = ['UPS', 'FedEx', 'DHL'];

/**
 * Shipping only handles PHYSICAL goods. Digital products (productId contains
 * "digital") complete at payment time and never enter this flow — that's what
 * gives us multiple distinct business-flow signatures for ariadne to observe.
 */
function requiresShipping(productId: string): boolean {
  return !productId.toLowerCase().includes('digital');
}

@Injectable()
export class ShippingService implements OnModuleInit {
  private readonly logger = new Logger(ShippingService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly prisma: PrismaService,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer connected');
  }

  async dispatchShipment(event: PaymentCompletedEvent) {
    if (!requiresShipping(event.productId)) {
      this.logger.log(
        `Order ${event.orderId} is a digital product (${event.productId}) — no shipment needed`,
      );
      return;
    }

    this.logger.log(
      `Payment completed for Order ${event.orderId} — dispatching shipment for ${event.productId}`,
    );

    const existing = await this.prisma.shipment.findUnique({
      where: { orderId: event.orderId },
    });

    if (existing) {
      this.logger.log(`Shipment for Order ${event.orderId} already exists — skipping`);
      return existing;
    }

    const carrier = CARRIERS[event.orderId.charCodeAt(0) % CARRIERS.length];
    const trackingNumber = `TRK-${event.orderId.slice(0, 8).toUpperCase()}`;

    const shipment = await this.prisma.shipment.create({
      data: {
        carrier,
        customerId: event.customerId,
        orderId: event.orderId,
        productId: event.productId,
        quantity: event.quantity,
        status: ShipmentStatus.DISPATCHED,
        trackingNumber,
      },
    });

    const dispatchedEvent: ShipmentDispatchedEvent = {
      carrier,
      createdAt: shipment.createdAt.toISOString(),
      customerId: event.customerId,
      eventId: uuid(),
      orderId: event.orderId,
      productId: event.productId,
      quantity: event.quantity,
      shipmentId: shipment.id,
      trackingNumber,
    };

    this.kafkaClient.emit(TOPICS.SHIPMENT_DISPATCHED, {
      key: event.orderId,
      value: JSON.stringify(dispatchedEvent),
    });

    this.logger.log(
      `Shipment #${shipment.id} dispatched for Order ${event.orderId} via ${carrier} (${trackingNumber}) — event published`,
    );
    return shipment;
  }

  async deliverShipment(event: ShipmentDispatchedEvent) {
    this.logger.log(
      `Shipment #${event.shipmentId} dispatched for Order ${event.orderId} — marking delivered`,
    );

    const shipment = await this.prisma.shipment.findUnique({
      where: { orderId: event.orderId },
    });

    if (!shipment) {
      this.logger.warn(`No shipment found for Order ${event.orderId} — skipping`);
      return;
    }

    if (shipment.status === ShipmentStatus.DELIVERED) {
      this.logger.log(`Shipment for Order ${event.orderId} already delivered — skipping`);
      return shipment;
    }

    const deliveredAt = new Date();
    const updated = await this.prisma.shipment.update({
      where: { orderId: event.orderId },
      data: { status: ShipmentStatus.DELIVERED, deliveredAt },
    });

    const deliveredEvent: ShipmentDeliveredEvent = {
      carrier: event.carrier,
      createdAt: shipment.createdAt.toISOString(),
      customerId: event.customerId,
      deliveredAt: deliveredAt.toISOString(),
      eventId: uuid(),
      orderId: event.orderId,
      shipmentId: shipment.id,
      trackingNumber: event.trackingNumber,
    };

    this.kafkaClient.emit(TOPICS.SHIPMENT_DELIVERED, {
      key: event.orderId,
      value: JSON.stringify(deliveredEvent),
    });

    this.logger.log(
      `Shipment #${shipment.id} delivered for Order ${event.orderId} — event published`,
    );
    return updated;
  }
}

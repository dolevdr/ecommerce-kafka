import {
  OrderCreatedEvent,
  PaymentCompletedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { OrderStatus } from '@prisma/client';
import { v4 as uuid } from 'uuid';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './create-order.dto';

@Injectable()
export class OrderService implements OnModuleInit {
  private readonly logger = new Logger(OrderService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly prisma: PrismaService,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer connected');
  }

  async createOrder(dto: CreateOrderDto) {
    const order = await this.prisma.order.create({
      data: {
        customerId: dto.customerId,
        productId: dto.productId,
        quantity: dto.quantity,
      },
    });

    await this.prisma.order.update({
      where: { orderId: order.orderId },
      data: { status: OrderStatus.PROCESSING },
    });

    const event: OrderCreatedEvent = {
      createdAt: order.createdAt.toISOString(),
      customerId: order.customerId,
      eventId: uuid(),
      orderId: order.orderId,
      productId: order.productId,
      quantity: order.quantity,
    };

    this.kafkaClient.emit(TOPICS.ORDERS_CREATED, {
      key: order.orderId,
      value: JSON.stringify(event),
    });

    this.logger.log(`Order ${order.orderId} created (PROCESSING) and event published`);
    return { ...order, status: OrderStatus.PROCESSING };
  }

  async completeOrder(event: PaymentCompletedEvent) {
    this.logger.log(
      `Payment completed for Order ${event.orderId} — updating status`,
    );

    const order = await this.prisma.order.findUnique({
      where: { orderId: event.orderId },
    });

    if (!order) {
      this.logger.warn(`Order ${event.orderId} not found — skipping`);
      return;
    }

    if (order.status === OrderStatus.COMPLETED || order.status === OrderStatus.FAILED) {
      this.logger.log(`Order ${event.orderId} already in terminal state (${order.status}) — skipping`);
      return order;
    }

    const updated = await this.prisma.order.update({
      where: { orderId: event.orderId },
      data: { status: OrderStatus.COMPLETED },
    });

    this.logger.log(`Order ${event.orderId} status updated to COMPLETED`);
    return updated;
  }

  async failOrder(orderId: string, reason: string) {
    this.logger.log(
      `Failing Order ${orderId} — reason: ${reason}`,
    );

    const order = await this.prisma.order.findUnique({
      where: { orderId },
    });

    if (!order) {
      this.logger.warn(`Order ${orderId} not found — skipping`);
      return;
    }

    if (order.status === OrderStatus.COMPLETED || order.status === OrderStatus.FAILED) {
      this.logger.log(`Order ${orderId} already in terminal state (${order.status}) — skipping`);
      return order;
    }

    const updated = await this.prisma.order.update({
      where: { orderId },
      data: { status: OrderStatus.FAILED },
    });

    this.logger.log(`Order ${orderId} status updated to FAILED`);
    return updated;
  }

  async shipOrder(orderId: string) {
    const order = await this.prisma.order.findUnique({ where: { orderId } });

    if (!order) {
      this.logger.warn(`Order ${orderId} not found — skipping ship`);
      return;
    }

    if (order.status === OrderStatus.FAILED || order.status === OrderStatus.DELIVERED) {
      this.logger.log(`Order ${orderId} in terminal state (${order.status}) — skipping ship`);
      return order;
    }

    const updated = await this.prisma.order.update({
      where: { orderId },
      data: { status: OrderStatus.SHIPPED },
    });

    this.logger.log(`Order ${orderId} status updated to SHIPPED`);
    return updated;
  }

  async deliverOrder(orderId: string) {
    const order = await this.prisma.order.findUnique({ where: { orderId } });

    if (!order) {
      this.logger.warn(`Order ${orderId} not found — skipping deliver`);
      return;
    }

    if (order.status === OrderStatus.FAILED || order.status === OrderStatus.DELIVERED) {
      this.logger.log(`Order ${orderId} in terminal state (${order.status}) — skipping deliver`);
      return order;
    }

    const updated = await this.prisma.order.update({
      where: { orderId },
      data: { status: OrderStatus.DELIVERED },
    });

    this.logger.log(`Order ${orderId} status updated to DELIVERED`);
    return updated;
  }
}

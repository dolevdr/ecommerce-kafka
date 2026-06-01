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

    this.logger.log(`Order ${order.orderId} created and event published`);
    return order;
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

    if (order.status === OrderStatus.COMPLETED) {
      this.logger.log(`Order ${event.orderId} already completed — skipping`);
      return order;
    }

    const updated = await this.prisma.order.update({
      where: { orderId: event.orderId },
      data: { status: OrderStatus.COMPLETED },
    });

    this.logger.log(`Order ${event.orderId} status updated to COMPLETED`);
    return updated;
  }
}

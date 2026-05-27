import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { TOPICS, OrderCreatedEvent } from '@ecommerce-kafka/shared';
import { PrismaService } from '../prisma/prisma.service';
import { CreateOrderDto } from './create-order.dto';
import { v4 as uuid } from 'uuid';

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
}

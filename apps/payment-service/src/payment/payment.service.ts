import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  InventoryReservedEvent,
  PaymentCompletedEvent,
  PaymentFailedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
import { PrismaService } from '../prisma/prisma.service';
import { v4 as uuid } from 'uuid';

@Injectable()
export class PaymentService implements OnModuleInit {
  private readonly logger = new Logger(PaymentService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly prisma: PrismaService,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer connected');
  }

  async processPayment(event: InventoryReservedEvent) {
    this.logger.log(
      `Received inventory reserved for Order ${event.orderId} — processing payment`,
    );

    const existing = await this.prisma.payment.findUnique({
      where: { orderId: event.orderId },
    });

    if (existing) {
      this.logger.log(`Payment for Order ${event.orderId} already processed — skipping`);
      return existing;
    }

    const amount = event.quantity * 100;

    if (amount > 300) {
      const payment = await this.prisma.payment.create({
        data: {
          amount,
          customerId: event.customerId,
          orderId: event.orderId,
          status: 'FAILED',
        },
      });

      const failedEvent: PaymentFailedEvent = {
        amount,
        createdAt: payment.createdAt.toISOString(),
        customerId: event.customerId,
        eventId: uuid(),
        orderId: event.orderId,
        reason: `Payment declined: amount ${amount} exceeds limit of 300`,
      };

      this.kafkaClient.emit(TOPICS.PAYMENT_FAILED, {
        key: event.orderId,
        value: JSON.stringify(failedEvent),
      });

      this.logger.warn(
        `Payment FAILED for Order ${event.orderId} — ${failedEvent.reason}`,
      );
      return payment;
    }

    const payment = await this.prisma.payment.create({
      data: {
        amount,
        customerId: event.customerId,
        orderId: event.orderId,
        status: 'COMPLETED',
      },
    });

    const paymentEvent: PaymentCompletedEvent = {
      amount: payment.amount,
      createdAt: payment.createdAt.toISOString(),
      customerId: payment.customerId,
      eventId: uuid(),
      orderId: payment.orderId,
    };

    this.kafkaClient.emit(TOPICS.PAYMENT_COMPLETED, {
      key: payment.orderId,
      value: JSON.stringify(paymentEvent),
    });

    this.logger.log(`Payment completed for Order ${event.orderId} — event published`);
    return payment;
  }
}

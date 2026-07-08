import { EventTracerModule } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { OrderModule } from '../order/order.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    EventTracerModule.forRoot({
      serviceName: 'order-service',
      tenantId: 'acme',
      transport: {
        kafka: { brokers: [process.env.KAFKA_BROKER || 'localhost:9092'] },
      },
      redaction: {
        allowlist: [
          'orderId',
          'customerId',
          'productId',
          'quantity',
          'amount',
          'reason',
          'eventId',
          'shipmentId',
          'carrier',
          'trackingNumber',
          'deliveredAt',
        ],
      },
    }),
    OrderModule,
  ],
})
export class AppModule {}

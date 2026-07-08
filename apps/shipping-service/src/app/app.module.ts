import { EventTracerModule } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ShippingModule } from '../shipping/shipping.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    EventTracerModule.forRoot({
      serviceName: 'shipping-service',
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
    ShippingModule,
  ],
})
export class AppModule {}

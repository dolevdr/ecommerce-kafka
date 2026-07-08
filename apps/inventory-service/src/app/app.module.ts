import { EventTracerModule } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { InventoryModule } from '../inventory/inventory.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    EventTracerModule.forRoot({
      serviceName: 'inventory-service',
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
        ],
      },
    }),
    InventoryModule,
  ],
})
export class AppModule {}

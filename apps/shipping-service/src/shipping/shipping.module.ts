import { EventTracerKafkaSerializer } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ShippingController } from './shipping.controller';
import { ShippingService } from './shipping.service';
import { PrismaService } from '../prisma/prisma.service';

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: 'KAFKA_CLIENT',
        inject: [EventTracerKafkaSerializer],
        useFactory: (serializer: EventTracerKafkaSerializer) => ({
          transport: Transport.KAFKA,
          options: {
            client: {
              brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
              clientId: 'shipping-service-client',
            },
            producer: {
              allowAutoTopicCreation: true,
            },
            serializer,
          },
        }),
      },
    ]),
  ],
  controllers: [ShippingController],
  providers: [ShippingService, PrismaService],
})
export class ShippingModule {}

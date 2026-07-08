import { EventTracerKafkaSerializer } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { OrderController } from './order.controller';
import { OrderService } from './order.service';
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
              clientId: 'order-service',
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
  controllers: [OrderController],
  providers: [OrderService, PrismaService],
})
export class OrderModule {}

import { EventTracerKafkaSerializer } from '@ariadne/sdk-nestjs';
import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { PaymentController } from './payment.controller';
import { PaymentService } from './payment.service';
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
              clientId: 'payment-service-client',
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
  controllers: [PaymentController],
  providers: [PaymentService, PrismaService],
})
export class PaymentModule {}

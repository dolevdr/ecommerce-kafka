import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app/app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
          clientId: 'inventory-service',
        },
        consumer: {
          groupId: 'inventory-service-group',
        },
        subscribe: {
          fromBeginning: false,
        },
      },
    },
  );

  await app.listen();
  Logger.log('Inventory Service is listening for Kafka events');
}

bootstrap();

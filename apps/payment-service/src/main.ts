import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app/app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        allowAutoTopicCreation: true,
        brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
        clientId: 'payment-service',
      },
      consumer: {
        groupId: 'payment-service-group',
      },
      subscribe: {
        fromBeginning: false,
      },
    },
  });

  await app.startAllMicroservices();

  const port = process.env.PAYMENT_SERVICE_PORT || 3003;
  await app.listen(port, '0.0.0.0');
  Logger.log(`Payment Service running on http://localhost:${port}`);
}

bootstrap();

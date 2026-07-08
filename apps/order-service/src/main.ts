import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app/app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter(),
  );

  app.useGlobalPipes(new ValidationPipe({ transform: true, whitelist: true }));

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        allowAutoTopicCreation: true,
        brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
        clientId: 'order-service-consumer',
      },
      consumer: {
        groupId: 'order-service-group',
      },
      subscribe: {
        fromBeginning: false,
      },
    },
  }, { inheritAppConfig: true });

  await app.startAllMicroservices();

  const port = process.env.ORDER_SERVICE_PORT || 3001;
  await app.listen(port, '0.0.0.0');
  Logger.log(`Order Service running on http://localhost:${port}`);
}

bootstrap();

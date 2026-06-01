import { type PaymentCompletedEvent, TOPICS } from '@ecommerce-kafka/shared';
import { Body, Controller, Post } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { CreateOrderDto } from './create-order.dto';
import { OrderService } from './order.service';

@Controller('orders')
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  createOrder(@Body() dto: CreateOrderDto) {
    return this.orderService.createOrder(dto);
  }

  @EventPattern(TOPICS.PAYMENT_COMPLETED)
  async handlePaymentCompleted(@Payload() event: PaymentCompletedEvent) {
    await this.orderService.completeOrder(event);
  }
}

import {
  type InventoryReservationFailedEvent,
  type PaymentCompletedEvent,
  type PaymentFailedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
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

  @EventPattern(TOPICS.INVENTORY_RESERVATION_FAILED)
  async handleInventoryFailed(@Payload() event: InventoryReservationFailedEvent) {
    await this.orderService.failOrder(event.orderId, event.reason);
  }

  @EventPattern(TOPICS.PAYMENT_COMPLETED)
  async handlePaymentCompleted(@Payload() event: PaymentCompletedEvent) {
    await this.orderService.completeOrder(event);
  }

  @EventPattern(TOPICS.PAYMENT_FAILED)
  async handlePaymentFailed(@Payload() event: PaymentFailedEvent) {
    await this.orderService.failOrder(event.orderId, event.reason);
  }
}

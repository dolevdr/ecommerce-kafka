import { type InventoryReservedEvent, TOPICS } from '@ecommerce-kafka/shared';
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { PaymentService } from './payment.service';

@Controller()
export class PaymentController {
  constructor(private readonly paymentService: PaymentService) {}

  @EventPattern(TOPICS.INVENTORY_RESERVED)
  async handleInventoryReserved(@Payload() event: InventoryReservedEvent) {
    await this.paymentService.processPayment(event);
  }
}

import {
  type PaymentCompletedEvent,
  type ShipmentDispatchedEvent,
  TOPICS,
} from '@ecommerce-kafka/shared';
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ShippingService } from './shipping.service';

@Controller()
export class ShippingController {
  constructor(private readonly shippingService: ShippingService) {}

  @EventPattern(TOPICS.PAYMENT_COMPLETED)
  async handlePaymentCompleted(@Payload() event: PaymentCompletedEvent) {
    await this.shippingService.dispatchShipment(event);
  }

  @EventPattern(TOPICS.SHIPMENT_DISPATCHED)
  async handleShipmentDispatched(@Payload() event: ShipmentDispatchedEvent) {
    await this.shippingService.deliverShipment(event);
  }
}

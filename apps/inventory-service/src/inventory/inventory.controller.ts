import { type OrderCreatedEvent, TOPICS } from '@ecommerce-kafka/shared';
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InventoryService } from './inventory.service';

@Controller()
export class InventoryController {
  constructor(private readonly inventoryService: InventoryService) {}

  @EventPattern(TOPICS.ORDERS_CREATED)
  async handleOrderCreated(@Payload() event: OrderCreatedEvent) {
    debugger;
    await this.inventoryService.reserveStock(event);
  }
}

export interface InventoryReservedEvent {
  createdAt: string;
  customerId: string;
  eventId: string;
  orderId: string;
  productId: string;
  quantity: number;
  reservationId: number;
}

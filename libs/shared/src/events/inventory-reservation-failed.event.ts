export interface InventoryReservationFailedEvent {
  createdAt: string;
  eventId: string;
  orderId: string;
  productId: string;
  quantity: number;
  reason: string;
}

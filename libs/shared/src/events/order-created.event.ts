export interface OrderCreatedEvent {
  createdAt: string;
  customerId: string;
  eventId: string;
  orderId: string;
  productId: string;
  quantity: number;
}

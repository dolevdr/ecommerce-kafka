export interface PaymentFailedEvent {
  amount: number;
  createdAt: string;
  customerId: string;
  eventId: string;
  orderId: string;
  reason: string;
}

export interface PaymentCompletedEvent {
  amount: number;
  createdAt: string;
  customerId: string;
  eventId: string;
  orderId: string;
}

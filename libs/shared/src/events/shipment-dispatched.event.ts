export interface ShipmentDispatchedEvent {
  carrier: string;
  createdAt: string;
  customerId: string;
  eventId: string;
  orderId: string;
  productId: string;
  quantity: number;
  shipmentId: number;
  trackingNumber: string;
}

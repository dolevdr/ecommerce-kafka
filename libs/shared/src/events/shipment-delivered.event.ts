export interface ShipmentDeliveredEvent {
  carrier: string;
  createdAt: string;
  customerId: string;
  deliveredAt: string;
  eventId: string;
  orderId: string;
  shipmentId: number;
  trackingNumber: string;
}

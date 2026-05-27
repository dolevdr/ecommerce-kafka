import { IsInt, IsPositive, IsString } from 'class-validator';

export class CreateOrderDto {
  @IsString()
  customerId: string;

  @IsString()
  productId: string;

  @IsInt()
  @IsPositive()
  quantity: number;

  constructor(customerId: string, productId: string, quantity: number) {
    this.customerId = customerId;
    this.productId = productId;
    this.quantity = quantity;
  }
}

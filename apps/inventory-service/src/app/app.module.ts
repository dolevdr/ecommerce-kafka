import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { InventoryModule } from '../inventory/inventory.module';

@Module({
  imports: [
    ConfigModule.forRoot(),
    InventoryModule,
  ],
})
export class AppModule {}

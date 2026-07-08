# Monitoring ecommerce-kafka with Ariadne (EventTracer)

This app is instrumented with [Ariadne](../ariadne)'s zero-touch NestJS SDK so a
single `POST /orders` produces one connected distributed trace across the
services (order → inventory → payment → shipping → order), visible in Ariadne's UI.

The app runs **four** Kafka pub/sub services and, depending on the product,
exercises **multiple distinct business flows** (see "Multiple flows" below) —
a deliberately branchy topology for exercising the tracer.

The business logic is untouched — only module configuration was added.

## What was changed

1. **Vendored SDK** — Ariadne's libs are unpublished, so they're packed as tarballs
   into `vendor/ariadne/` and referenced as `file:` deps in `package.json`:
   `@ariadne/protocol`, `@ariadne/transport-core`, `@ariadne/transport-kafka`,
   `@ariadne/sdk-nestjs`. Refresh them with `./scripts/sync-ariadne-sdk.sh` whenever
   Ariadne changes.

2. **Per-service wiring** (order, inventory, payment, shipping):
   - `app/app.module.ts` — imports `EventTracerModule.forRoot({ serviceName, tenantId: 'acme', transport: { kafka: { brokers } }, redaction })`. This registers a **global consumer interceptor** (traces every `@EventPattern` handler — no controller changes) and exports the producer serializer.
   - `<domain>/<domain>.module.ts` — the Kafka producer client switched from `ClientsModule.register` to `registerAsync`, injecting `EventTracerKafkaSerializer` as `options.serializer` (traces every `kafkaClient.emit(...)`). Business emit calls are unchanged.
   - `main.ts` — `connectMicroservice(..., { inheritAppConfig: true })`. **Required**: in a hybrid HTTP+microservice app, the global interceptor is otherwise not applied to Kafka message handlers, and traces fragment (each hop starts a new trace).

## How tracing flows

- The producer serializer stamps W3C-style trace headers (`x-trace-id`,
  `x-span-id`, `x-parent-span-id`, ...) on each Kafka message and emits a
  PRODUCER span.
- The consumer interceptor extracts those headers on the receiving service,
  opens an AsyncLocalStorage context, and emits a CONSUMER span whose parent is
  the upstream producer span — so the `parentSpanId` chain stitches services
  into one trace.
- Spans are emitted fire-and-forget to the `_tracing` Kafka topic. Ariadne's
  collector consumes `_tracing` → Postgres; the API + UI visualize it.

## Running the full setup

Single Kafka broker on `localhost:9092` serves both business topics and
`_tracing`. Ariadne's Postgres (`:5432`) backs the collector/API.

**1. Infra (once):**
```bash
# ecommerce-kafka's Kafka (or Ariadne's — either works, just one broker on :9092)
docker compose up -d            # in this repo, OR `docker compose up -d kafka` in ariadne
```

**2. Ariadne backend** (from the `ariadne` repo, Node 22):
```bash
./scripts/monitor-ecommerce.sh  # postgres + migrate + collector(:3101)/api(:3000)/ui(:4200)
```
The collector consumes `_tracing` from `localhost:9092` (`COLLECTOR_KAFKA_BROKERS`).

> Port note: Ariadne's collector defaults to `:3001`, which clashes with
> order-service. `monitor-ecommerce.sh` runs it on `:3101`; alternatively keep the
> order-service on a free port (this repo's `.env` uses `ORDER_SERVICE_PORT=3004`).

**3. This app** (Node 20):
```bash
KAFKA_BROKER=localhost:9092 npx nx run-many -t serve \
  --projects=order-service,inventory-service,payment-service,shipping-service
```
Ports (`.env`): inventory `:3002`, payment `:3003`, order `:3004`, shipping `:3005`.

**4. Trigger a flow and view it:**
```bash
curl -s -X POST http://localhost:3004/orders \
  -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","productId":"prod-1","quantity":2}'
```
Wait ~2s, then open the Ariadne UI at http://localhost:4200 — the new trace spans
all three services. Or query the API:
```bash
curl -s -H 'x-tenant-id: acme' 'http://localhost:3000/api/traces?limit=1'
```

Expected: for a physical product, one trace, `rootService: order-service`,
`spanCount: 12`, spanning order → inventory → payment → shipping → order.

## Multiple flows

The **shipping-service** is conditional, so different orders take different paths —
each a distinct business-flow signature for Ariadne to discover:

| # | Trigger (`productId` / `quantity`) | Path | End state | Spans |
|---|---|---|---|---|
| 1 | physical, e.g. `widget-blue` / `2` | order → inventory.reserved → payment.completed → **shipment.dispatched → shipment.delivered** | `DELIVERED` | 12 |
| 2 | digital, `productId` contains `digital`, e.g. `digital-ebook` / `1` | order → inventory.reserved → payment.completed | `COMPLETED` | 7 |
| 3 | any, `quantity > 5` | order → **inventory.reservation-failed** | `FAILED` | ~4 |
| 4 | physical/digital, `quantity` s.t. `amount = quantity*100 > 300` (e.g. `4`) | order → inventory.reserved → **payment.failed** → release + fail | `FAILED` | ~7 |

Shipping decision: a product **requires shipping unless its `productId` contains
`"digital"`** (see `shipping-service/src/shipping/shipping.service.ts`). Digital
orders complete at payment time and never enter the shipping flow.

Two extra fan-outs make the topology richer for the tracer:
`payment.completed` is consumed by **both** order-service and shipping-service, and
`shipment.dispatched` by **both** shipping-service (→ deliver) and order-service (→ SHIPPED).

Trigger both primary flows:
```bash
# physical → ships (DELIVERED)
curl -s -X POST localhost:3004/orders -H 'content-type: application/json' \
  -d '{"customerId":"c1","productId":"widget-blue","quantity":2}'
# digital → no shipping (COMPLETED)
curl -s -X POST localhost:3004/orders -H 'content-type: application/json' \
  -d '{"customerId":"c2","productId":"digital-ebook","quantity":1}'
```

## The shipping-service

- **Subscribes** `payment.completed` → if the product is physical, creates a
  `Shipment` (DISPATCHED) and **publishes** `shipment.dispatched`.
- **Subscribes** `shipment.dispatched` (its own) → marks the shipment DELIVERED and
  **publishes** `shipment.delivered`.
- order-service additionally **subscribes** `shipment.dispatched` (→ `SHIPPED`) and
  `shipment.delivered` (→ `DELIVERED`).

New Kafka topics `shipment.dispatched` / `shipment.delivered` are added to
`docker-compose.yml`'s `init-kafka`. New Prisma `Shipment` model + `ShipmentStatus`
enum and `SHIPPED`/`DELIVERED` `OrderStatus` values were added via the
`add_shipments` migration. `PaymentCompletedEvent` now carries `productId`/`quantity`
so shipping can make the physical/digital decision.

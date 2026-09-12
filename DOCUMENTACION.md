# Orderbook Vibranium — Documentación técnica

Documentación funcional y técnica del proyecto: qué resuelve, cómo está
construido, qué hace cada componente y cómo probarlo de punta a punta.

> Este documento complementa el `README.md` (portada en inglés). Aquí se explica
> el proyecto en detalle y en español.

---

## 1. Qué es este proyecto

Es un **libro de órdenes (order book)** que permite a los usuarios enviar
órdenes de **compra** y **venta** de un activo llamado **Vibranium**, valorado
en pesos colombianos (COP). El sistema:

- Recibe órdenes límite (precio + cantidad) de compra y venta.
- Las **casa (matchea)** siguiendo prioridad **precio-tiempo**.
- **Acredita y debita las billeteras** (wallets) de los usuarios a medida que se
  ejecutan las operaciones.
- Mantiene un **historial de operaciones** (trazabilidad).

Está escrito en **Go**. El núcleo de dominio (order book, wallets, reglas) usa
solo la librería estándar; las dependencias externas viven exclusivamente en los
adaptadores de infraestructura.

Gracias a la arquitectura hexagonal, el mismo binario corre en **dos modos**, y
la elección es solo configuración:

| Modo | Cómo se levanta | Infraestructura |
|---|---|---|
| **Completo** (por defecto en Docker) | `make up` | Redpanda (event log), Postgres (wallets/trades/orders/journal), Redis (caché) |
| **En memoria** | `make run` o `make up-memory` | Ninguna: todo en proceso, cero dependencias |

### Alcance

**Implementado y verificado:** órdenes límite de compra/venta, cancelación,
matching precio-tiempo, reserva/liquidación/liberación de fondos, historial de
trades, un solo activo, la arquitectura de log ordenado, tests, prueba de carga,
y los **adaptadores reales de infraestructura** (Redpanda + Postgres + Redis) con
journal de idempotencia, durabilidad ante reinicios y fallo cerrado ante caída de
la base de datos.

**Fuera de alcance (por diseño, para evitar overengineering):** interfaz gráfica,
login/registro de usuarios, órdenes de mercado, y la **reconstrucción del libro
por replay al arrancar** (más snapshots), que es la limitación conocida descrita
en la sección 9.

---

## 2. Concepto: cómo funciona un order book

Un order book es un mecanismo de **subasta doble continua**: compradores y
vendedores publican sus intenciones y el sistema las cruza automáticamente
cuando los precios coinciden.

- **Bids (ofertas de compra):** "compro hasta Q unidades a precio P o menor".
- **Asks (ofertas de venta):** "vendo hasta Q unidades a precio P o mayor".
- **Mejor bid:** el precio de compra más alto disponible.
- **Mejor ask:** el precio de venta más bajo disponible.

**Invariante central:** un libro en reposo nunca está "cruzado" (el mejor bid es
siempre menor que el mejor ask). Si una orden entrante cruza el mercado, se
**ejecuta de inmediato**; lo que no se puede ejecutar, queda **reposando** en el
libro.

**Maker vs Taker:**
- **Maker:** su precio no cruza, así que descansa en el libro y aporta liquidez.
- **Taker:** su precio cruza, así que consume liquidez existente al instante.

**Prioridad precio-tiempo:** primero casan las órdenes con mejor precio; a igual
precio, casa primero la que llegó antes (FIFO).

**Precio de ejecución:** la operación se cierra al **precio del maker** (la orden
que ya estaba en el libro). Por eso un comprador puede pagar menos que su límite:
si reservó al precio límite, la diferencia se le devuelve.

---

## 3. Arquitectura

La clave del desafío es una paradoja: **el libro no tolera concurrencia** (el
matching debe ser estrictamente ordenado y determinista), pero **miles de
órdenes pueden llegar en el mismo milisegundo**. La solución, igual que en bolsas
reales (LMAX, Nasdaq), es **serializar todas las órdenes en un flujo ordenado y
correr un motor de matching de un solo hilo**.

```
  Clientes / bots
        │  POST /orders
        ▼
┌──────────────────┐   reserva fondos (atómica)   ┌─────────────────────────────┐
│   API HTTP       │─────────────────────────────▶│  Wallet store                │
│  (sin estado,    │   bloqueo de fila por user    │  (available / locked)        │
│   escalable)     │            place              │  memory | POSTGRES (+REDIS)  │
└───────┬──────────┘──────────────┐               └─────────────────────────────┘
        │                         ▼
        │              ┌────────────────────────┐   emite eventos
        │              │  Motor de matching      │───────────────┐
        │              │  1 goroutine, dueño del │                │
        │              │  libro, precio-tiempo   │                ▼
        │              └────────────────────────┘     ┌──────────────────────┐
        │                                             │  Event log           │
        │                                             │  memory | REDPANDA   │
        │                                             │  1 partición/símbolo │
        │                                             └───────┬──────────────┘
        ▼                                                     ▼
  GET /book, /trades, /wallets              ┌─────────────────────────────────┐
                                            │  Settlement (consumer)           │
                                            │  journal de idempotencia (ID ev.)│
                                            │  aplica trades, libera reservas  │
                                            └───────┬─────────────────────────┘
                                                    ├─▶ Wallet store
                                                    ├─▶ Trade history
                                                    └─▶ Order projection
                                                        memory | POSTGRES
```

En mayúsculas están los adaptadores durables que cablea `make up`. Todo se elige
por variables de entorno; el código del núcleo es idéntico en ambos modos.

### Principios de diseño

1. **Un solo goroutine es dueño del libro.** Todas las mutaciones pasan por un
   único canal, así que no hay locks ni condiciones de carrera sobre el libro.
   El orden se decide una sola vez —en el canal— y luego se aplica de forma
   determinista.
2. **El log ordenado es un puerto, no un canal.** Una sola interfaz `EventLog`
   con dos implementaciones: un canal de Go en memoria, y una partición de
   Redpanda/Kafka particionada por símbolo para preservar el orden por
   instrumento. Ambas están implementadas y funcionando.
3. **El motor nunca toca el dinero.** Solo decide matches y emite eventos. Un
   componente separado (**settlement**) aplica créditos/débitos y devoluciones.
   Esto mantiene el hot path pequeño y el camino del dinero auditable.

### Dos puntos de serialización (importante entenderlos)

- **Seguridad del dinero:** la **reserva de fondos** en el `WalletStore` (bajo
  mutex) es el punto que previene el doble gasto. Ninguna orden entra al libro
  sin una reserva exitosa.
- **Orden del matching:** el **canal del motor** decide el orden de ejecución.

Son dos mecanismos distintos con propósitos distintos: uno protege saldos, el
otro protege el determinismo del matching.

---

## 4. Componentes

El proyecto sigue una **arquitectura hexagonal (puertos y adaptadores)**: un
núcleo puro (dominio + casos de uso) rodeado de puertos (interfaces) y
adaptadores (infraestructura). Las dependencias apuntan **hacia adentro**: los
adaptadores dependen del núcleo, nunca al revés.

| Capa | Paquete | Responsabilidad |
|---|---|---|
| Raíz | `cmd/orderbook` | Composition root: cablea adaptadores con servicios, apagado ordenado, healthcheck. |
| **Núcleo** | `internal/core/domain` | Entidades y reglas: `Order`, `Trade`, `Wallet`, `Event` y el libro de órdenes puro. |
| **Núcleo** | `internal/core/port` | Interfaces: puertos inbound (driving) y outbound (driven). |
| **Núcleo** | `internal/core/service` | Casos de uso: trading, settlement, market data, wallets. |
| Adaptador (driving) | `internal/adapter/driving/rest` | Adaptador HTTP (`net/http`), sin estado; llama a los puertos inbound. |
| Adaptador (driven) | `internal/adapter/driven/matching` | Motor de matching de un solo goroutine; implementa `MatchingEngine`. |
| Adaptador (driven) | `internal/adapter/driven/memstore` | Repos + journal en memoria. |
| Adaptador (driven) | `internal/adapter/driven/memlog` | Event log ordenado en memoria; implementa `EventLog`. |
| Adaptador (driven) | `internal/adapter/driven/postgres` | Repos durables + journal de idempotencia (bloqueo de fila, `schema.sql`). |
| Adaptador (driven) | `internal/adapter/driven/kafkalog` | Event log durable sobre Redpanda/Kafka. |
| Adaptador (driven) | `internal/adapter/driven/rediscache` | **Decorador** de caché de lectura sobre cualquier `WalletRepository`. |
| Infra | `internal/bootstrap` | Elige los adaptadores según la configuración. |
| Infra | `internal/config` | Configuración por variables de entorno. |
| Infra | `scripts/loadtest` | Cliente concurrente de bots + verificador del invariante de saldos. |

**Puertos** (en `internal/core/port`):
- *Inbound (driving):* `TradingService`, `MarketDataService`, `WalletService` —
  lo que la app ofrece; los implementan los servicios y los llama el adaptador REST.
- *Outbound (driven):* `MatchingEngine`, `WalletRepository`, `TradeRepository`,
  `OrderRepository`, `EventLog`, `EventJournal` — lo que la app necesita; los
  implementan los adaptadores driven.

Cada puerto tiene dos implementaciones intercambiables, así que la
infraestructura es una **decisión de configuración, no un cambio de código**:

| Puerto | `memory` | real |
|---|---|---|
| `EventLog` | canal de Go | **Redpanda/Kafka**, 1 partición por símbolo |
| `WalletRepository` | mapa con `RWMutex` | **Postgres** + `SELECT … FOR UPDATE` |
| `TradeRepository` | slice | **Postgres** append-only |
| `OrderRepository` | mapa | **Postgres** (monotónico por secuencia) |
| `EventJournal` | set | **Postgres** `processed_events` |
| caché | — | **Redis** como decorador |

El adaptador de Redis merece mención: es un **decorador** que implementa
`WalletRepository` y envuelve al durable. La caché se compone en el composition
root, y ni el núcleo ni Postgres saben que existe. Nunca es autoritativa: las
escrituras invalidan la clave y `List` (reconciliación) siempre va a la fuente de
verdad.

### 4.1 Núcleo — `core/domain` (el corazón del negocio)

Paquete puro, sin dependencias de transporte ni almacenamiento. Todo lo demás
depende de él; él no depende de nada más que la librería estándar.

- **`Order`**: orden límite (id, usuario, lado BUY/SELL, precio, cantidad,
  estado, cantidad ejecutada, secuencia). Todos los montos son `int64` para que
  la aritmética sea **exacta** (sin drift de punto flotante). Estados:
  `OPEN`, `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`, `REJECTED`.
- **`Wallet`**: saldo del usuario dividido en **available** (disponible) y
  **locked** (reservado), tanto para COP como para Vibranium. Ese split es lo
  que evita el doble gasto.
- **`Trade`**: match ejecutado entre una orden maker y una taker. Es la unidad
  atómica de liquidación y la base de la trazabilidad.
- **`Event`**: item del flujo de salida del motor (`ORDER_ACCEPTED`, `TRADE`,
  `ORDER_UPDATED`). Settlement y las proyecciones se construyen consumiéndolo.
- **`OrderBook`** (`orderbook.go`): estructura pura y determinista. Dos lados
  (`bids`, `asks`), cada uno con niveles de precio ordenados (mejor primero) y
  colas FIFO por nivel. Métodos `Place`, `Cancel`, `Snapshot`. No tiene locks:
  la serialización la aporta el adaptador de matching.

### 4.2 Núcleo — `core/port` (los puertos)

Solo interfaces. Definen el contrato entre el núcleo y el exterior:
- **Inbound (driving):** `TradingService`, `MarketDataService`, `WalletService`.
- **Outbound (driven):** `MatchingEngine`, `WalletRepository`, `TradeRepository`,
  `OrderRepository`, `EventLog`/`EventPublisher`.

### 4.3 Núcleo — `core/service` (los casos de uso)

Implementan los puertos inbound y orquestan el dominio + los puertos outbound.
Sin lógica de transporte ni de storage.

- **`Trading`**: coloca y cancela órdenes. Contiene la orquestación crítica:
  **reserva fondos de forma atómica** (el chequeo de riesgo que evita el doble
  gasto) ANTES de que la orden entre al motor, y ejecuta una **acción
  compensatoria** (release) si el motor no la puede aceptar.
- **`MarketData`**: profundidad del libro (`Book`) e historial (`RecentTrades`).
- **`Wallets`**: siembra/lee wallets (`Seed`, `Get`, `List`).
- **`Settlement`**: consume el flujo de eventos y es el **único** que mueve
  dinero liquidado. En un `TRADE` descuenta/acredita a comprador y vendedor y
  registra el trade; en un `ORDER_UPDATED` terminal libera fondos sobrantes
  (sobre-reserva del comprador o remanente cancelado). Como los eventos llegan
  en orden de secuencia por un único flujo, las mutaciones son consistentes.

### 4.4 Adaptador driven — `adapter/driven/matching`

**`Engine`**: dueño de un `domain.OrderBook`. Recibe comandos (`place`,
`cancel`, `snapshot`) por un canal y los procesa en un solo goroutine (`Run`).
Implementa el puerto `MatchingEngine` (`Place`, `Cancel`, `Depth`) con una API
síncrona que bloquea hasta obtener respuesta. Asigna la secuencia de eventos
dentro del goroutine y los publica vía el puerto `EventPublisher`.

### 4.5 Adaptador driven — `adapter/driven/memstore` (proyecciones)

Implementaciones en memoria de los repositorios:
- **`WalletStore`**: saldos. `Update(userID, fn)` aplica una mutación atómica
  (con rollback si falla). Usa un `RWMutex`; en producción sería Postgres con
  bloqueo de fila / versión optimista.
- **`TradeStore`**: historial append-only de trades (trazabilidad).
- **`OrderStore`**: proyección del estado de cada orden para consultarla sin
  tocar el libro caliente del motor.

### 4.6 Adaptador driven — `adapter/driven/memlog` (log ordenado)

**`Log`**: implementa el puerto `EventLog` (`Publish`, `Events`, `Close`) con un
canal con buffer que absorbe ráfagas para que el motor nunca se bloquee por un
consumidor lento. Pensado para reemplazarse por Kafka/Kinesis sin tocar el núcleo.

### 4.7 Adaptador driving — `adapter/driving/rest` (capa HTTP)

`net/http` con enrutamiento por método+patrón (Go 1.22+). Es **fino**: mapea el
DTO del request a un comando del puerto inbound, delega en los servicios y mapea
resultados/errores a códigos HTTP. Ya no contiene lógica de negocio (la reserva
de fondos y la compensación viven en el servicio `Trading`).

---

## 5. Modelo de correctness: reservar / liquidar / liberar

El saldo se divide en **available** y **locked**. Esto es lo que previene el
doble gasto cuando un bot dispara muchas órdenes en el mismo milisegundo.

| Evento | COP | Vibranium |
|---|---|---|
| Colocar **BUY** `qty @ precio` | `qty*precio` pasa de available → locked | — |
| Colocar **SELL** `qty` | — | `qty` pasa de available → locked |
| Se ejecuta trade (`qty @ precioEjec`) | comprador: locked → gastado; vendedor: +available | vendedor: locked → entregado; comprador: +available |
| Devolución por sobre-reserva de compra | `qty*(límite − precioEjec)` locked → available | — |
| Cancelar remanente | locked → available | locked → available |

**Invariante del dinero:** la suma `available + locked` de COP y de Vibranium
sobre todos los usuarios es **constante**. Nada se crea ni se destruye. Los tests
de concurrencia y la prueba de carga lo verifican.

---

## 6. API HTTP

Puerto por defecto: `3000`. Todas las respuestas son JSON.

| Método | Ruta | Propósito |
|---|---|---|
| `POST` | `/wallets` | Crear/reemplazar una wallet (para pruebas; el registro está fuera de alcance). |
| `GET` | `/wallets` | Listar todas las wallets (reconciliación). |
| `GET` | `/wallets/{id}` | Obtener una wallet. |
| `POST` | `/orders` | Colocar una orden límite de compra/venta. |
| `DELETE` | `/orders/{id}` | Cancelar una orden en reposo. |
| `GET` | `/orders/{id}` | Estado de una orden. |
| `GET` | `/book?depth=N` | Profundidad agregada del libro. |
| `GET` | `/trades?limit=N` | Trades recientes (trazabilidad). |
| `GET` | `/health` | Liveness + conteo de trades. |

### Cuerpos de request

Colocar orden (`POST /orders`):

```json
{ "userId": "alice", "side": "BUY", "price": 100, "quantity": 50 }
```

- `side`: `"BUY"` o `"SELL"`.
- `price` y `quantity`: enteros positivos.

Sembrar wallet (`POST /wallets`):

```json
{ "userId": "alice", "copAvailable": 1000000, "vibraniumAvailable": 0 }
```

### Códigos de respuesta relevantes

- `201 Created`: orden aceptada (puede haber ejecutado total o parcialmente, o
  quedar en reposo).
- `422 Unprocessable Entity`: fondos insuficientes → la orden se marca
  `REJECTED` y no entra al libro.
- `404 Not Found`: wallet u orden inexistente.
- `503 Service Unavailable`: el motor no está disponible (la reserva se revierte).

---

## 7. Cómo ejecutar

Requisitos: **Docker** (para el stack completo) o **Go 1.26+** (para correrlo pelado).

### Arquitectura completa — un solo comando

```bash
make up          # docker compose up --build -d, espera a que esté healthy
```

Levanta cuatro servicios y aplica el esquema de base de datos automáticamente
(es idempotente, no hay contenedor de migraciones que correr):

| Servicio | Rol | Puerto |
|---|---|---|
| `orderbook` | API + motor de matching + settlement | 3000 |
| `redpanda` | log de eventos ordenado y durable (API Kafka) | 19092 |
| `postgres` | wallets, trades, orders, journal de settlement | 5432 |
| `redis` | caché de lectura de wallets | 6379 |
| `console` | UI web para inspeccionar el event log (opcional) | 8080 |

### Sin infraestructura

El mismo binario, con todos los adaptadores en `memory`:

```bash
make run         # proceso pelado en :3000
make up-memory   # un solo contenedor, sin dependencias
```

Comandos útiles del Makefile:

```bash
make up / down / reset   # levantar / bajar / bajar y borrar volúmenes
make logs / ps           # seguir logs / ver salud de servicios
make test                # todos los tests con -race
make loadtest            # prueba de carga + reconciliación de saldos
make psql                # shell de psql
make topic / groups      # inspeccionar el topic y el lag de settlement
make replay-test         # forzar replay total y probar que no se duplica dinero
```

Variables de entorno principales (ver `.env.example` para la lista completa):

| Variable | Default | Descripción |
|---|---|---|
| `PORT` / `HOST` | `3000` / `0.0.0.0` | Bind HTTP. |
| `SYMBOL` | `VIB` | Símbolo del instrumento. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |
| `ENGINE_BUFFER` | `65536` | Buffer del canal del motor (backpressure). |
| `EVENT_LOG_DRIVER` | `memory` | `memory` \| `kafka`. |
| `WALLET_STORE_DRIVER` | `memory` | `memory` \| `postgres`. |
| `TRADE_STORE_DRIVER` | `memory` | `memory` \| `postgres`. |
| `ORDER_STORE_DRIVER` | `memory` | `memory` \| `postgres`. |
| `CACHE_DRIVER` | `none` | `none` \| `redis`. |
| `KAFKA_BROKERS` | `localhost:19092` | Lista separada por comas. |
| `DATABASE_URL` | `postgres://orderbook:orderbook@localhost:5432/orderbook` | Conexión a Postgres. |
| `REDIS_URL` | `redis://localhost:6379` | Conexión a Redis. |
| `INFRA_WAIT_TIMEOUT` | `60s` | Espera de dependencias al arrancar. |

---

## 8. Cómo realizar pruebas

Hay tres niveles de prueba: **tests automatizados**, **prueba de carga con
verificación de saldos**, y **prueba manual end-to-end con curl**.

### 8.1 Tests automatizados

```bash
make test
# equivalente a:
go test ./... -race -count=1
```

- `-race` activa el detector de condiciones de carrera (clave en un sistema
  concurrente).
- `-count=1` desactiva el cache de tests para forzar ejecución real.

Tests que existen en el proyecto:
- `internal/core/domain/orderbook_test.go`: matching precio-tiempo, fills
  parciales, cancelaciones.
- `internal/core/domain/wallet_test.go`: reservar/liquidar/liberar y el invariante.
- `internal/core/service/settlement_test.go`: flujo completo motor + settlement
  a través del servicio de trading, incluida la devolución por sobre-reserva y
  el test de concurrencia que conserva el valor.

Para correr un paquete específico o ver detalle:

```bash
go test ./internal/core/domain/ -race -v
go test ./internal/core/service/ -race -v
```

### 8.2 Prueba de carga (valida saldos, como la evaluación)

Esta es la prueba que replica cómo evalúa Mercado Libre: dispara miles de
órdenes concurrentes y al final **reconcilia los saldos** para probar que no se
creó ni destruyó valor.

En una terminal:

```bash
make run
```

En otra:

```bash
go run ./scripts/loadtest -url http://localhost:3000 -pairs 5000 -concurrency 200
```

Parámetros:

| Flag | Default | Descripción |
|---|---|---|
| `-url` | `http://localhost:3000` | URL base de la API. |
| `-pairs` | `5000` | Número de pares comprador/vendedor. |
| `-concurrency` | `200` | Workers concurrentes. |
| `-price` | `100` | Precio límite de todas las órdenes. |
| `-qty` | `1` | Cantidad por orden. |

Qué hace, paso a paso:
1. **Siembra** `pairs` compradores y `pairs` vendedores con saldo justo.
2. **Dispara** todas las órdenes concurrentemente. Un run totalmente casado
   produce exactamente `pairs` trades.
3. Espera a que **settlement** drene y **verifica el invariante**: el total de
   COP y de Vibranium debe ser idéntico al inicial.

Salida esperada (ejemplo):

```
placed=10000 failed=0 in 1.2s (8300 orders/sec)
INVARIANT: totalCOP=... (expected ...) totalVibranium=... (expected ...) stillLocked=0
RESULT: OK — value conserved, no money created or destroyed
```

Si ves `RESULT: OK` y `stillLocked=0`, todo casó y el dinero cuadra.

> Nota sobre la escala: el enunciado pide 5000 **trades**/s. Como cada trade
> requiere dos órdenes que crucen, eso equivale a ~10.000 órdenes/s. El load test
> razona en órdenes; ajusta `-pairs` para el volumen que quieras medir.

### 8.3 Prueba manual end-to-end con curl

Un flujo mínimo que demuestra un trade y los créditos/débitos:

```bash
# 1) Sembrar dos usuarios
curl -s localhost:3000/wallets -d '{"userId":"alice","copAvailable":1000000,"vibraniumAvailable":0}'
curl -s localhost:3000/wallets -d '{"userId":"bob","copAvailable":0,"vibraniumAvailable":100}'

# 2) Bob vende 50 @ 100 (queda en reposo en el libro)
curl -s localhost:3000/orders -d '{"userId":"bob","side":"SELL","price":100,"quantity":50}'

# 3) Ver el libro: debe aparecer un ask de 50 @ 100
curl -s "localhost:3000/book?depth=5"

# 4) Alice compra 50 @ 100 -> cruza y ejecuta el trade
curl -s localhost:3000/orders -d '{"userId":"alice","side":"BUY","price":100,"quantity":50}'

# 5) Ver el historial de trades (trazabilidad)
curl -s "localhost:3000/trades?limit=10"

# 6) Verificar saldos
curl -s localhost:3000/wallets/alice   # +50 Vibranium, -5000 COP
curl -s localhost:3000/wallets/bob     # -50 Vibranium, +5000 COP
```

Prueba de **price improvement** (devolución por sobre-reserva):

```bash
# Bob vende barato: 10 @ 90
curl -s localhost:3000/orders -d '{"userId":"bob","side":"SELL","price":90,"quantity":10}'

# Alice compra dispuesta a pagar hasta 100. Ejecuta al precio del maker (90).
# Reserva 10*100=1000 COP pero solo gasta 10*90=900; se le devuelven 100 COP.
curl -s localhost:3000/orders -d '{"userId":"alice","side":"BUY","price":100,"quantity":10}'
curl -s localhost:3000/wallets/alice
```

Prueba de **cancelación** (libera lo reservado):

```bash
# Colocar una compra que no cruza (queda en reposo) y capturar su id
curl -s localhost:3000/orders -d '{"userId":"alice","side":"BUY","price":50,"quantity":10}'
# -> copiar el "id" de la respuesta

# Cancelar por id: el COP reservado (50*10=500) vuelve a available
curl -s -X DELETE localhost:3000/orders/ord-XXXXXXXX
curl -s localhost:3000/wallets/alice
```

> Consistencia: `POST /orders` responde de forma síncrona desde el motor, pero
> los saldos y la proyección de órdenes los actualiza settlement de forma
> asíncrona. Justo después de un POST puede haber un desfase de milisegundos
> antes de que `GET /wallets/{id}` o `GET /orders/{id}` reflejen el resultado.
> Con el log en Kafka ese desfase cruza la red, por lo que el load test **espera**
> a que settlement drene antes de reconciliar, en vez de asumir un retardo fijo.

### 8.4 Pruebas de resiliencia (stack real)

Estas son las que responden a "¿qué pasa cuando los componentes fallan?".

**a) Durabilidad ante reinicio de la app**

```bash
docker compose exec postgres psql -U orderbook -d orderbook -c "SELECT count(*) FROM trades;"
docker compose restart orderbook
sleep 12
curl -s localhost:3000/health          # el conteo de trades sobrevive
curl -s localhost:3000/wallets/alice   # el saldo sobrevive
```

**b) Idempotencia ante replay completo del log**

Es la prueba más fuerte del sistema: se rebobina el consumer group al offset 0,
forzando la reentrega de **todos** los eventos. Sin journal, el dinero se
duplicaría.

```bash
make replay-test
```

Resultado esperado: los totales antes y después son idénticos. Para verlo por
dentro:

```bash
make groups   # el offset volvió a avanzar hasta el final (reconsumió todo)
docker compose exec postgres psql -U orderbook -d orderbook \
  -c "SELECT count(*) FROM processed_events;"   # no creció: todo era duplicado
```

**c) Falla cerrado con la base de datos caída**

```bash
docker compose stop postgres
curl -i localhost:3000/orders -d '{"userId":"alice","side":"BUY","price":100,"quantity":1}'
# -> HTTP 503 "wallet store unavailable"; ninguna orden entra al libro
curl -s localhost:3000/health          # -> status "degraded"
docker compose start postgres          # recupera solo, sin reiniciar la app
```

**d) Trazabilidad del event log**

Con el stack arriba, abre <http://localhost:8080> (Redpanda Console) y mira el
topic `orderbook.events`: cada trade y cada actualización de orden está ahí, en
orden, con su ID único.

---

## 9. Modos de fallo (qué pasa cuando algo falla)

Estos son comportamientos **verificados** del stack corriendo, no intenciones:

- **Replay / entrega duplicada de eventos.** El log es at-least-once: el mismo
  evento puede llegar dos veces (reinicio del consumidor, rebalanceo, offset sin
  commitear). Cada evento lleva un ID único (`<producerID>-<secuencia>`) y
  settlement lo registra en el **journal de idempotencia** en Postgres antes de
  aplicarlo. Reposicionando el consumer group al offset 0 y reconsumiendo el log
  completo, los saldos quedan **idénticos**:
  ```bash
  make replay-test
  ```
  Verificado: se reconsumieron 20.004 eventos y `processed_events` no creció ni un
  registro; ni saldos ni trades se duplicaron.
- **Cae la app / reinicio.** Wallets, trades, orders y el journal viven en
  Postgres, así que el estado sobrevive. `docker compose restart orderbook`
  conserva todos los saldos y el historial completo.
- **Wallet store caído.** La API **falla cerrado**: `POST /orders` devuelve `503`
  (`wallet store unavailable`) y ninguna orden entra al libro, porque ninguna
  puede entrar sin reserva exitosa. `/health` reporta `degraded`. Cuando Postgres
  vuelve, el pool reconecta y el trading se reanuda sin reiniciar nada.
- **Falla la API tras reservar fondos.** El `release` compensatorio corre con un
  contexto desacoplado (el del request puede estar ya cancelado, que suele ser la
  causa del fallo), así que los fondos nunca quedan atrapados en `locked`.
- **Dependencia lenta al arrancar.** Cada adaptador reintenta hasta
  `INFRA_WAIT_TIMEOUT`, y el arranque **se niega a iniciar** en lugar de degradar
  silenciosamente un libro contable a memoria no durable.
- **Backpressure.** `Publish` entrega el evento a una cola interna que drena una
  goroutine productora, así la latencia del broker nunca bloquea al motor
  single-writer. Si la cola se satura, el motor se bloquea: backpressure
  intencional, no pérdida silenciosa.

> **Limitación conocida, dicha sin rodeos:** el libro en memoria no se reconstruye
> al reiniciar. Recuperar las órdenes en reposo requiere reproducir el log sobre
> un libro nuevo (más snapshots periódicos para acotar el tiempo de replay). El
> log, los IDs de evento y el journal ya están en su lugar para hacerlo posible;
> la rutina de replay queda deliberadamente fuera del alcance del MVP.

---

## 10. Escalabilidad

- **Capa API:** sin estado, escala horizontalmente detrás de un balanceador. Como
  la reserva de fondos toma un **bloqueo de fila en Postgres** y no un mutex en
  proceso, la garantía de no-doble-gasto sobrevive a múltiples réplicas de la API.
- **Un solo activo (Vibranium):** una sola partición y un motor ya superan el
  objetivo. Medido en laptop contra el stack completo (Redpanda + Postgres +
  Redis): **~10.000–14.900 órdenes/s (≈5.000–7.400 trades/s)**, con el invariante
  del dinero conservado. El matching en memoria no es el cuello de botella.
- **Más activos:** se particiona por **símbolo** entre particiones/motores →
  escalado lineal por instrumento. Un único símbolo muy caliente es el techo
  duro: no se puede paralelizar un solo libro. Es una propiedad de los order
  books, no una limitación del diseño.
- **Siguiente cuello de botella:** los round trips por evento de settlement a
  Postgres. Batchear (o usar una transacción unit-of-work por trade) es la
  siguiente optimización natural.
- **En producción gestionada:** Redpanda → MSK/Confluent, Postgres → Aurora,
  Redis → ElastiCache. Son cambios de URL, no de código: los adaptadores ya
  hablan los protocolos estándar.

---

## 11. Observabilidad

Lo que hay hoy:

- **Logs estructurados en JSON** vía `slog`, incluyendo una línea al arrancar con
  los adaptadores seleccionados (útil para saber en qué modo está corriendo).
- **`/health` refleja el estado de las dependencias**: si el trade store no
  responde devuelve `503` con `status: degraded` en vez de mentir `ok`.
- **Redpanda Console** en <http://localhost:8080> para ver el event log crudo:
  cada trade y actualización de orden, en orden, con su ID único. Es la
  trazabilidad hecha visible.
- **`make groups`** muestra el lag del consumer group de settlement.

Siguiente paso natural: métricas (órdenes/s, latencia de match, profundidad del
libro, lag del log, latencia de settlement) expuestas en `/metrics`, y un job
periódico de **reconciliación** que pruebe el invariante del dinero en producción.

---

## 12. Estructura del proyecto

```
cmd/orderbook                       composition root: cablea adaptadores con servicios, apagado ordenado

internal/core/                      NÚCLEO (sin dependencias de transporte/almacenamiento)
  domain/                           entidades + reglas: order, trade, wallet, events, order book puro
  port/                             interfaces: puertos inbound (driving) + outbound (driven)
  service/                          casos de uso: trading, settlement, market data, wallets

internal/adapter/                   INFRAESTRUCTURA (implementa los puertos)
  driving/rest/                     adaptador HTTP (net/http, sin estado) -> llama puertos inbound
  driven/matching/                  motor de matching single-goroutine     -> MatchingEngine
  driven/memstore/                  repos + journal en memoria             -> repositorios
  driven/memlog/                    event log ordenado en memoria          -> EventLog
  driven/postgres/                  repos durables + journal (schema.sql, bloqueo de fila)
  driven/kafkalog/                  event log durable sobre Redpanda/Kafka
  driven/rediscache/                decorador de caché de lectura de wallets

internal/bootstrap                  elige los adaptadores según configuración
internal/config                     configuración por variables de entorno
scripts/loadtest                    cliente de bots concurrente + verificador del invariante de saldos
```

Regla de dependencias: todo apunta **hacia adentro**. `adapter/*` depende de
`core/port` y `core/domain`; `core/service` depende de `core/port` y
`core/domain`; `core/domain` no depende de nadie. El núcleo no conoce HTTP,
canales ni almacenamiento concretos.

---

## 13. Referencias y estado del arte

Estas son las implementaciones y artículos que se revisaron al investigar el
problema. Se listan con lo que aporta cada una y, sobre todo, en qué se
diferencian de las decisiones de este proyecto: el contraste es lo que justifica
por qué acá se eligió otra cosa.

> Contenido parafraseado a partir de las fuentes enlazadas; los detalles y el
> código original están en cada enlace.

### Implementaciones de order book en Go

**[i25959341/orderbook](https://github.com/i25959341/orderbook)** — *Matching
Engine for Limit Order Book in Golang* (MIT).
La referencia más completa del ecosistema Go. Implementa prioridad precio-tiempo,
órdenes límite y de mercado, cancelación, y reporta más de 300.000 trades por
segundo. Su API (`ProcessLimitOrder`, `ProcessMarketOrder`, `CancelOrder`) documenta
con diagramas los casos de fill total, parcial y remanente en reposo.
*Diferencia:* usa `shopspring/decimal` para precios y cantidades; acá se optó por
`int64` exacto, aceptando la pérdida de fracciones a cambio de aritmética sin
asignaciones ni redondeo en el hot path. También es solo el libro: no modela
billeteras ni liquidación.

**[danielgatis/go-orderbook](https://github.com/danielgatis/go-orderbook)** —
libro de órdenes límite para HFT (MIT), basado en el clásico artículo de WK Selph
sobre estructuras de datos para order books. Expone `Depth()` para la profundidad
agregada, igual que el `Snapshot()` de este proyecto.

**[ricardohsd/order-book](https://github.com/ricardohsd/order-book)** — librería
Go de libro de órdenes límite para exchanges de cripto. El propio autor advierte
que es un proyecto personal no usado en producción. Útil como implementación
mínima de contraste.

**[bhomnick — Building an exchange limit order book in Go](https://bhomnick.net/building-a-simple-limit-order-in-go/)**
— el más interesante desde el punto de vista de diseño.
Usa un **arreglo preasignado indexado por precio** (`prices [MAX_PRICE]*PricePoint`)
con listas enlazadas por nivel, logrando inserción, cancelación y fill en O(1).
La cancelación es *lazy*: pone la cantidad en cero y la ignora al recorrer, en vez
de sacar la orden de la estructura. Reporta entre 350.000 y 2 millones de
acciones/segundo.
*Diferencia:* ese arreglo obliga a acotar el rango de precios de antemano y
consume memoria proporcional a ese rango, no a las órdenes vivas. Acá se usa
`map[int64]*priceLevel` más un slice de precios ordenado con búsqueda binaria:
O(log n) para insertar un nivel nuevo en lugar de O(1), pero sin techo de precio
y con memoria proporcional a los niveles realmente ocupados.
*Coincidencia notable:* la sección "next steps" del artículo propone exactamente
la arquitectura que este proyecto implementa — escribir las acciones a un log tipo
Kafka para poder reconstruir el libro tras una caída, y desacoplar la liquidación
del motor de matching. Es una validación independiente del diseño.

### Estructuras de datos para niveles de precio

**[Aditya Raj — Market Depth Simplified: Building an Order Book Engine in Go](https://medium.com/@adityaraj_201551/market-depth-simplified-building-an-order-book-engine-in-go-9abb9bcaec9a)**
(nov 2024) y su repo
**[aditya201551/in-memory-order-book-go](https://github.com/aditya201551/in-memory-order-book-go)**.
Explica por qué un **B-tree** (vía `google/btree`) encaja bien con un libro de
órdenes: mantiene los precios ordenados y permite consultas por rango eficientes,
que es justo lo que se necesita cuando el mejor precio cambia en cada tick.
*Diferencia:* el artículo usa `float64` para precios y cantidades. Para dinero eso
introduce error de redondeo acumulativo, y es precisamente lo que este proyecto
evita con enteros (ver la nota de representación en `core/domain/order.go`). La
elección de B-tree sí sería el próximo paso natural acá si el número de niveles de
precio creciera mucho: reemplazaría el slice ordenado sin tocar el resto del motor.

### El patrón pipeline

**[Majid Imanzade — Building Efficient Order Book Processing with Go's Pipeline Pattern](https://medium.com/@majidimanzade1/building-efficient-order-book-processing-with-gos-pipeline-pattern-10b5e752029a)**
(dic 2025).
Construye un pipeline de etapas conectadas por canales, donde cada etapa devuelve
un canal que consume la siguiente: filtrar pares válidos → enriquecer con órdenes
pendientes → ordenar y recortar → publicar. Usa fan-out con `WaitGroup` dentro de
las etapas pesadas de I/O.
*Diferencia importante de alcance:* ese pipeline **arma y publica snapshots de
profundidad**, no ejecuta el matching. Son problemas distintos: una proyección de
lectura sí se puede paralelizar libremente, mientras que un libro de órdenes exige
un orden total y por eso acá el matching vive en una sola goroutine.
El pipeline de este proyecto es
`API → [canal] → motor → [event log] → settlement → stores`, y su etapa lenta es
settlement, no el matching (ver sección 10, *Escalabilidad*).

### Qué no cubre ninguna de estas referencias

Todas resuelven el **libro de órdenes**; ninguna modela el **dinero**. No hay
billeteras, ni saldo disponible frente a reservado, ni créditos y débitos al
ejecutarse una operación, ni liquidación desacoplada. Ese es justamente el centro
del desafío ("que efectúe los créditos y débitos correctamente"), y es la parte
que en este proyecto vive en `core/domain/wallet.go` y `core/service/settlement.go`
bajo el modelo reservar / liquidar / liberar de la sección 5.

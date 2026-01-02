[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=buildspace-run_demo-microservices-crypto-exchange-persister&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=buildspace-run_demo-microservices-crypto-exchange-persister)

# Crypto Price Persister Microservice

A Spring Boot microservice that consumes real-time cryptocurrency price updates from RabbitMQ and persists them to PostgreSQL using **dynamic batch processing** with elastic connection pooling. Built following **Hexagonal Architecture** principles.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Infrastructure                           │
│  ┌─────────────┐              ┌────────────────────────┐        │
│  │  RabbitMQ   │              │      PostgreSQL        │        │
│  │   Broker    │              │       Database         │        │
│  └──────┬──────┘              └──────────┬─────────────┘        │
│         │                                │                      │
│  ┌──────▼──────────┐           ┌─────────▼─────────────┐        │
│  │   RabbitMQ      │           │    JDBC Elastic       │        │
│  │   Consumer      │           │    Pool Adapter       │        │
│  │   (Adapter)     │           │    (Adapter)          │        │
│  └──────┬──────────┘           └──────────▲────────────┘        │
└─────────┼─────────────────────────────────┼─────────────────────┘
          │                                 │
┌─────────▼─────────────────────────────────┼─────────────────────┐
│                      Application          │                     │
│  ┌──────────────────────────────┐    ┌────┴───────────────┐     │
│  │     StorePrice Service       │────│  PriceEventStore   │     │
│  │     (Use Case)               │    │      (Port)        │     │
│  └──────────────────────────────┘    └────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## ✨ Features

- **Dynamic batch processing** - Adaptive batch size (25-500) based on queue load
- **Elastic connection pooling** - Automatic scaling of DB connections (2-10)
- **High throughput** - Optimized for bulk message processing
- **Backpressure handling** - Prefetch control to prevent memory overflow
- **Manual ACK** - Ensures message reliability and no data loss
- **Observability** - Prometheus metrics for monitoring
- **Hexagonal Architecture** - Clean separation of concerns
- **Schema initialization** - Automatic database schema creation

## 🚀 Getting Started

### Prerequisites

- Java 21+
- Docker & Docker Compose
- Maven 3.9+
- PostgreSQL 15+
- RabbitMQ 3.12+

### Running Locally

1. **Run the application:**
   ```bash
   mvn spring-boot:run
   ```
   
   Spring Boot will automatically:
   - Detect and start Docker Compose services (`compose.yml`)
   - Initialize PostgreSQL and RabbitMQ
   - Run RabbitMQ topology setup (exchanges, queues, bindings, DLQ)
   - Wait for all services to be healthy
   - Start the consumer application
   
   > **Note:** Services remain running after the app stops. Use `docker-compose down` to stop them.

2. **Check health:**
   ```bash
   curl http://localhost:8081/actuator/health
   ```

**Access Services:**
- RabbitMQ Management UI: http://localhost:15672 (credentials: myuser/secret)
- PostgreSQL: localhost:5432 (credentials: myuser/secret)

### Environment Variables

The application supports the following environment variables for configuration:

| Variable            | Description                    | Default           |
|---------------------|--------------------------------|-------------------|
| `DB_USERNAME`       | PostgreSQL username            | `myuser`          |
| `DB_PASSWORD`       | PostgreSQL password            | `secret`          |
| `RABBITMQ_HOST`     | RabbitMQ broker hostname       | `localhost`       |
| `RABBITMQ_PORT`     | RabbitMQ AMQP port             | `5672`            |
| `RABBITMQ_USER`     | RabbitMQ username              | `guest`           |
| `RABBITMQ_PASSWORD` | RabbitMQ password              | `guest`           |
| `RABBITMQ_VHOST`    | RabbitMQ virtual host          | `/`               |
| `PRICES_QUEUE`      | Queue name for price updates   | `currency-update` |
| `BATCH_SIZE`        | Initial batch size (25-500)    | `25`              |

Example:
```bash
export DB_USERNAME=myuser
export DB_PASSWORD=mypassword
export RABBITMQ_HOST=rabbitmq.example.com
export BATCH_SIZE=50
mvn spring-boot:run
```

### Running Tests

**Unit tests only (default profile):**
```bash
mvn test
```

**Integration tests only (smoke tests with Testcontainers):**
```bash
mvn test -Psmoke-tests
```

**All tests:**
```bash
mvn test -Punit-tests,smoke-tests
```

> **Note:** Integration tests use Testcontainers for PostgreSQL and RabbitMQ. Tests run concurrently for faster execution.

## 📊 Observability

### Available Metrics

Spring Boot Actuator exposes the following metrics automatically:

| Metric                           | Type  | Description                           | Source    |
|----------------------------------|-------|---------------------------------------|-----------|
| `hikaricp.connections.active`    | Gauge | Number of active DB connections       | HikariCP  |
| `hikaricp.connections.idle`      | Gauge | Number of idle DB connections         | HikariCP  |
| `hikaricp.connections.min`       | Gauge | Minimum pool size                     | HikariCP  |
| `hikaricp.connections.max`       | Gauge | Maximum pool size                     | HikariCP  |
| `spring.rabbitmq.listener`       | Timer | RabbitMQ listener execution time      | Spring    |
| `jvm.memory.used`                | Gauge | JVM memory usage                      | Micrometer|
| `system.cpu.usage`               | Gauge | System CPU usage                      | Micrometer|

> **Note:** Custom metrics for batch processing (batch size, messages processed/failed) are not yet implemented.

### Endpoints

- **Health:** `GET /actuator/health`
- **Metrics:** `GET /actuator/metrics`
- **Prometheus:** `GET /actuator/prometheus`

### Monitoring Best Practices

**Database Connection Pool:**
```promql
# Alert when pool exhaustion is near
(hikaricp_connections_active / hikaricp_connections_max) > 0.8
```

**JVM Memory Usage:**
```promql
# Alert on high memory usage
(jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"}) > 0.9
```

## 🛠️ Technology Stack

- **Framework:** Spring Boot 3.4
- **Messaging:** RabbitMQ with Spring AMQP
- **Database:** PostgreSQL with JDBC (no JPA/Hibernate for performance)
- **Connection Pool:** HikariCP with elastic configuration
- **Observability:** Micrometer + Prometheus
- **Testing:** JUnit 5 + Mockito + Testcontainers + Awaitility
- **Build Tool:** Maven 3.9+

## 📁 Project Structure

```
src/main/java/run/buildspace/crypto/price/consumer/
├── domain/
│   ├── model/              # Domain entities
│   │   └── PriceUpdate     # Price update value object
│   └── exception/          # Domain exceptions
│       ├── InvalidPriceException
│       └── InvalidSymbolException
├── application/
│   ├── ports/
│   │   ├── in/             # Input ports (use cases)
│   │   │   └── ForStorePrice
│   │   └── out/            # Output ports (driven)
│   │       └── PriceEventStore
│   └── services/           # Application services
│       └── StorePrice      # Main business logic
└── infrastructure/
     ── adapters/
        ├── in/             # Driving adapters
        │   └── messaging/  
        │       ├── RabbitMQConsumer
        │       └── MessageBatch
        └── out/            # Driven adapters
            └── persistence/
                └── JdbcElasticPool

```

## 🎯 Domain Model

### Key Entities

- **`PriceUpdate`**: Represents a real-time price update from the message broker
  - `symbol`: Cryptocurrency symbol (e.g., "BTC")
  - `price`: Current price (validated to be positive)
  - `timestamp`: Event timestamp in milliseconds (validated to not be in the future)

### Validation Rules

- **Symbol**: Must not be null or blank
- **Price**: Must be positive (> 0)
- **Timestamp**: Must not be in the future

## ⚡ Performance Optimization

### Dynamic Batch Processing

The consumer uses **adaptive batch sizing** to optimize throughput:

- **Initial batch size**: 25 messages (configurable via `BATCH_SIZE`)
- **Maximum batch size**: 500 messages
- **Scaling strategy**: 
  - Increases batch size when queue has pending messages
  - Decreases batch size when queue is empty
  - Prevents memory overflow with prefetch limits

### Elastic Connection Pooling

The JDBC connection pool scales dynamically:

- **Minimum idle connections**: 2
- **Maximum pool size**: 10
- **Connection timeout**: 30 seconds
- **Idle timeout**: 10 minutes
- **Max lifetime**: 30 minutes

**Formula for optimal pool size:**
```
connections = ((core_count × 2) + effective_spindle_count)
```

### Prefetch Configuration

- **Prefetch count**: 100 messages
- **Purpose**: Balance between throughput and memory usage
- **Recommendation**: Adjust based on message size and available RAM

## 🔧 Configuration Tuning

### For High Throughput Scenarios

When you need to maximize message processing rate:

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        prefetch: 200  # Loads more messages into memory in advance
                       # Reduces network round-trips to RabbitMQ
                       # Allows batch processor to form larger batches faster
                       
  datasource:
    hikari:
      maximum-pool-size: 20  # More concurrent DB connections available
                             # Prevents connection wait times during heavy load
                             # Each batch operation can get a connection immediately

rabbitmq:
  batch-size: 100  # Starts with larger batches from the beginning
                   # Reduces number of database round-trips
                   # Each INSERT batch has more data, improving DB efficiency
```

**Trade-off:** Higher memory usage (messages buffered in JVM) and more database resources.

---

### For Low Memory Environments

When running with limited RAM or shared resources:

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        prefetch: 25  # Fewer messages buffered in memory
                      # Reduces JVM heap pressure
                      # Prevents OutOfMemoryError on large messages
                      
  datasource:
    hikari:
      maximum-pool-size: 5  # Fewer connection objects in memory
                            # Each connection has overhead (buffers, state)
                            # Reduces total resource consumption

rabbitmq:
  batch-size: 10  # Smaller batches = less data held in memory at once
                  # Faster processing of each individual batch
                  # More frequent commits reduce transaction memory
```

**Trade-off:** Lower throughput due to more network round-trips and smaller database batches.

---

### For Development/Testing

When you need visibility and easy debugging:

```yaml
logging:
  level:
    run.buildspace.crypto.price.consumer: DEBUG  # Shows detailed app behavior
    org.springframework.jdbc: DEBUG  # Logs every SQL statement executed
                                     # Helps identify N+1 queries or inefficient SQL

spring:
  sql:
    init:
      mode: always  # Recreates tables on every startup
                    # Fresh state for each test run
                    # No need to manually clean database
```

**Note:** Never use `mode: always` in production—it will drop your data!

## 🧪 Test Coverage

### Unit Tests
- **Domain validations**: Price, symbol, timestamp
- **Service logic**: Store price use case
- **Batch processing**: Message batching algorithm
- **Connection pool**: Elastic scaling behavior

### Integration Tests
- **End-to-end flow**: RabbitMQ → Service → PostgreSQL
- **Database persistence**: Verify data integrity
- **Concurrent processing**: Multiple consumers
- **Error handling**: Invalid messages, DB failures
- **Testcontainers**: Real RabbitMQ and PostgreSQL instances

## 🚨 Error Handling

### Message Processing Failures

```
Invalid Message → NACK + Requeue → Retry
                ↓ (After max retries)
          Dead Letter Queue
```

### Database Failures

```
Connection Error → Retry with backoff
Transaction Error → Rollback + NACK
Connection Pool Exhausted → Block until available
```

### Poison Messages

Messages that consistently fail should be:
1. Logged with full details
2. Sent to error exchange/DLQ
3. Alerted for manual investigation

## 📝 Database Schema

The application uses the `schema.sql` file to initialize the database:

```sql
CREATE TABLE PRICE_HISTORY (
    SYMBOL VARCHAR(20) NOT NULL,
    TS BIGINT NOT NULL,
    PRICE NUMERIC(20, 8) NOT NULL,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SYMBOL, TS),
    CONSTRAINT PRICE_POSITIVE CHECK (PRICE > 0),
    CONSTRAINT TS_POSITIVE CHECK (TS > 0)
);
```

### Schema Details

- **Primary Key**: Composite key `(SYMBOL, TS)` ensures unique price per symbol per timestamp
- **`SYMBOL`**: Cryptocurrency trading pair (e.g., "BTC")
- **`TS`**: Unix timestamp in milliseconds (BIGINT for precision)
- **`PRICE`**: Price with 8 decimal precision (standard for crypto)
- **`CREATED_AT`**: Server-side timestamp when record was inserted
- **Constraints**:
  - `PRICE_POSITIVE`: Ensures price is always greater than 0
  - `TS_POSITIVE`: Ensures timestamp is always valid (> 0)

### Query Optimization

The composite primary key `(SYMBOL, TS)` automatically creates an index that optimizes:
- Queries filtering by symbol: `WHERE SYMBOL = 'BTC'`
- Time-range queries: `WHERE SYMBOL = 'BTC' AND TS BETWEEN x AND y`
- Latest price queries: `WHERE SYMBOL = 'BTC' ORDER BY TS DESC LIMIT 1`

## 🔍 Troubleshooting

### Consumer Not Processing Messages

**Symptoms:** Messages stuck in queue, no database writes

**Solutions:**
1. Check RabbitMQ connection:
   ```bash
   docker logs <consumer-container>
   ```
2. Verify queue binding:
   ```bash
   rabbitmqctl list_bindings
   ```
3. Check database connection:
   ```bash
   curl http://localhost:8081/actuator/health
   ```

### Database Connection Pool Exhausted

**Symptoms:** `Connection is not available` errors

**Solutions:**
1. Increase pool size in `application.yml`
2. Reduce batch size to lower concurrency
3. Check for connection leaks (unclosed connections)

### High Memory Usage

**Symptoms:** Out of memory errors, GC pressure

**Solutions:**
1. Reduce prefetch count
2. Reduce batch size
3. Increase heap size: `-Xmx2g`

### Slow Batch Processing

**Symptoms:** Low throughput, increasing queue depth

**Solutions:**
1. Add database indexes on frequently queried columns
2. Optimize batch insert statements
3. Increase database connection pool
4. Consider partitioning PRICE_HISTORY table by time range

##  Production Considerations

### Security

- ✅ Use encrypted connections (TLS/SSL) for RabbitMQ
- ✅ Enable SSL for PostgreSQL connections
- ✅ Store credentials in secrets manager (Vault, AWS Secrets Manager)


### Reliability

- ✅ Configure Dead Letter Queue for poison messages
- ✅ Set up monitoring alerts for queue depth
- ✅ Enable database connection validation


### Scalability

- ✅ Deploy multiple consumer instances for horizontal scaling
- ✅ Archive old data to cold storage

## 📚 Related Documentation

- [Spring AMQP Documentation](https://docs.spring.io/spring-amqp/docs/current/reference/html/)
- [RabbitMQ Best Practices](https://www.rabbitmq.com/best-practices.html)
- [HikariCP Configuration](https://github.com/brettwooldridge/HikariCP#configuration-knobs-baby)
- [PostgreSQL Performance Tuning](https://wiki.postgresql.org/wiki/Performance_Optimization)

## 📝 License

This project is for educational/demo purposes.

---

## 🤝 Integration with Producer

This consumer works in tandem with the **Crypto Exchange Reader** (producer) microservice:

1. **Producer** connects to Binance WebSocket and publishes price updates to RabbitMQ
2. **Consumer** reads from RabbitMQ and persists price data to PostgreSQL
3. Producer manages subscriptions; Consumer persists price history

**Message Flow:**
```
Binance WebSocket → Producer → RabbitMQ → Consumer → PostgreSQL
                       ↓                              ↓
                  Subscription DB              Price History
```

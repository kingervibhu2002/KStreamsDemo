# KStreamsDemo — Complete Project Documentation

## What This Project Does

This project is a **Kafka Streams** playground built on **Spring Boot**. It simulates an
e-commerce purchase pipeline where every purchase event is consumed and aggregated in
real time into a per-user summary (count, total spend, max single purchase). The
aggregated data is stored in **RocksDB** on disk and can be queried instantly via a
REST API without going back to Kafka — a Kafka Streams feature called **Interactive Queries**.

---

## High-Level Data Flow

```
REST API (POST /api/purchases/random)
         │
         ▼
  PurchaseProducerService
  (KafkaTemplate → Spring Kafka JsonSerializer)
         │
         ▼
  ┌─────────────────────────────────────┐
  │          purchases topic             │  ← Kafka broker
  │  key = userId  value = Purchase JSON │
  └─────────────────────────────────────┘
         │
         ▼  (Kafka Streams embedded consumer)
  PurchaseTopologyConfig
  groupByKey → aggregate
         │
         ├──► RocksDB state store ("user-stats-store")   ← local disk on app server
         │         queried by StoreQueryService
         │         exposed via GET /api/stats
         │
         └──► user-stats topic (changelog / output)      ← Kafka broker
```

> **Important:** RocksDB does NOT live on the Kafka broker. It lives on the same
> machine/container/pod that runs the Spring Boot app. Kafka only stores the
> raw events and a compacted changelog backup of the store.

---

## Project Structure

```
KStreamsDemo/
├── src/
│   ├── main/
│   │   ├── java/org/example/
│   │   │   ├── KStreamsDemoApplication.java      ← Spring Boot entry point
│   │   │   ├── Main.java                          ← delegates to KStreamsDemoApplication
│   │   │   ├── Topics.java                        ← shared constants (topic/store names)
│   │   │   ├── config/
│   │   │   │   └── PurchaseTopologyConfig.java    ← Kafka Streams topology definition
│   │   │   ├── model/
│   │   │   │   ├── Purchase.java                  ← input event model
│   │   │   │   └── UserStats.java                 ← aggregated state model
│   │   │   ├── serde/
│   │   │   │   └── JsonSerde.java                 ← JSON serializer/deserializer
│   │   │   ├── service/
│   │   │   │   ├── PurchaseProducerService.java   ← produces events to Kafka
│   │   │   │   └── StoreQueryService.java         ← reads from RocksDB store
│   │   │   └── web/
│   │   │       └── StatsController.java           ← REST API controller
│   │   └── resources/
│   │       └── application.yml                    ← all configuration
│   └── test/
│       └── java/org/example/
│           └── StreamsTopologyTest.java           ← unit test (no broker needed)
├── k8s/
│   ├── namespace.yml                              ← K8s namespace
│   ├── configmap.yml                              ← K8s environment config
│   ├── kafka.yml                                  ← Kafka broker on K8s (dev only)
│   ├── statefulset.yml                            ← app deployment on K8s
│   └── service.yml                                ← K8s network services
├── Dockerfile                                     ← multi-stage Docker build
├── docker-compose.yml                             ← local container dev setup
└── pom.xml                                        ← Maven dependencies
```

---

## Java Classes — Detailed Explanation

---

### `KStreamsDemoApplication.java`
**Package:** `org.example`

The Spring Boot entry point. Three annotations do all the heavy lifting:

| Annotation | What it does |
|-----------|--------------|
| `@SpringBootApplication` | Enables component scanning, auto-configuration, and property binding |
| `@EnableKafkaStreams` | Tells Spring Kafka to create a `StreamsBuilderFactoryBean` which manages the `KafkaStreams` lifecycle (start, stop, rebalance) automatically |
| `@EnableScheduling` | Activates `@Scheduled` methods — used by `StoreQueryService` to log the store every 5 seconds |

You never call `KafkaStreams.start()` or `KafkaStreams.close()` manually. Spring handles it.

---

### `Topics.java`
**Package:** `org.example`

A constants-only class. Centralises all topic and store names so that the producer,
topology, and consumer all refer to the same strings without duplication.

| Constant | Value | Purpose |
|----------|-------|---------|
| `PURCHASES` | `"purchases"` | Input topic — one record per purchase event |
| `USER_STATS` | `"user-stats"` | Output topic — changelog of aggregated stats |
| `USER_STATS_STORE` | `"user-stats-store"` | Name of the RocksDB state store |
| `BOOTSTRAP_SERVERS` | `"localhost:9092"` | Used only by the standalone `ProducerApp` |

---

### `model/Purchase.java`
**Package:** `org.example.model`

A Java **record** representing a single purchase event arriving on the `purchases` topic.

```
{ "userId": "alice", "item": "laptop", "amount": 999.99 }
```

| Field | Type | Description |
|-------|------|-------------|
| `userId` | `String` | The Kafka record **key** — determines which partition this event goes to |
| `item` | `String` | What was purchased |
| `amount` | `double` | Purchase value in dollars |

Because `userId` is the key, all purchases for the same user always land on the same
partition and are processed by the same stream thread — this is what makes per-user
aggregation correct without any cross-partition coordination.

---

### `model/UserStats.java`
**Package:** `org.example.model`

A Java **record** representing the running aggregate stored in RocksDB for each user.
This is the **value type of the KTable**.

| Field | Type | Description |
|-------|------|-------------|
| `count` | `long` | How many purchases this user has made |
| `total` | `double` | Sum of all purchase amounts |
| `maxAmount` | `double` | Largest single purchase |

**Two key methods:**

- `empty()` — the **initializer** called by Kafka Streams when a user is seen for the
  first time. Returns `(count=0, total=0.0, maxAmount=0.0)`.
- `add(Purchase p)` — the **aggregator** called for every new purchase. Returns a new
  `UserStats` with updated fields. Records are immutable (record = no setters), so
  each call produces a fresh object.

```
First purchase by alice ($5.00):
  empty()          → UserStats(0, 0.0, 0.0)
  .add($5.00)      → UserStats(1, 5.0, 5.0)

Second purchase by alice ($15.00):
  .add($15.00)     → UserStats(2, 20.0, 15.0)
```

---

### `serde/JsonSerde.java`
**Package:** `org.example.serde`

Kafka Streams needs to **serialise** (Java object → bytes) and **deserialise**
(bytes → Java object) every key and value that crosses a topic or gets written to a
state store. A `Serde` is simply the pair of serializer + deserializer bundled together.

This class implements `Serde<T>` using **Jackson** (`ObjectMapper`) to convert objects
to/from JSON bytes. It is generic — the same class works for both `Purchase` and
`UserStats`:

```java
JsonSerde.of(Purchase.class)   // used for the input stream
JsonSerde.of(UserStats.class)  // used for the state store and output topic
```

A single static `ObjectMapper` is shared across all instances (thread-safe and cheap).

---

### `config/PurchaseTopologyConfig.java`
**Package:** `org.example.config`

This is the **heart of the application**. It defines the Kafka Streams topology as a
Spring `@Configuration` bean. Spring Kafka wires in the `StreamsBuilder` automatically
when `@EnableKafkaStreams` is present.

**Step-by-step topology walkthrough:**

```java
// Step 1 — Read the purchases topic as a stream
KStream<String, Purchase> purchases = streamsBuilder.stream(
    "purchases",
    Consumed.with(Serdes.String(), JsonSerde.of(Purchase.class))
);
```
Creates a `KStream` — an unbounded sequence of purchase events. Each record has
`userId` as the key and a `Purchase` object as the value.

```java
// Step 2 — Declare a named, persistent (RocksDB) state store
KeyValueBytesStoreSupplier storeSupplier =
    Stores.persistentKeyValueStore("user-stats-store");
```
Declares the store explicitly by name. This makes it queryable later via Interactive
Queries. If you used an implicit store, you could not query it by name.

```java
// Step 3 — Group by key and aggregate
KTable<String, UserStats> userStats = purchases
    .groupByKey(...)
    .aggregate(
        UserStats::empty,                            // initializer
        (userId, purchase, agg) -> agg.add(purchase), // aggregator
        Materialized.as(storeSupplier)...
    );
```
- `groupByKey` — groups all records with the same `userId` together (same key = same
  partition = same thread, so this is efficient with no network shuffle)
- `aggregate` — for each incoming `Purchase`, calls `agg.add(purchase)` and writes the
  result back to RocksDB
- The result is a `KTable` — a table where each row is the latest aggregate for one user

```java
// Step 4 — Emit changes to the output topic
userStats.toStream().to("user-stats", Produced.with(...));
```
Every time a user's `UserStats` changes, the new value is emitted to the `user-stats`
topic. This also acts as a **changelog** — if RocksDB is lost, Kafka Streams can replay
this topic to rebuild the store from scratch.

**Why KTable and not just KStream?**
A `KTable` is a **materialised view** — it holds the current state. A `KStream` is just
events. The KTable is what makes it possible to ask "what is alice's total right now?"
without scanning all historical events.

---

### `service/PurchaseProducerService.java`
**Package:** `org.example.service`

A Spring `@Service` that produces `Purchase` events to the `purchases` topic using
Spring Kafka's `KafkaTemplate`.

| Method | Description |
|--------|-------------|
| `send(Purchase)` | Sends a specific purchase to Kafka, keyed by `userId` |
| `sendRandom()` | Generates a random purchase (random user, item, amount), sends it, and returns it |

`KafkaTemplate` is auto-configured by Spring Boot using the serializer settings in
`application.yml`. The value serializer is Spring Kafka's `JsonSerializer` which uses
Jackson to produce the same JSON bytes as our `JsonSerde`.

The `userId` is passed as the **record key**. Kafka uses the key to determine which
partition the record goes to (via a hash). Records with the same key always go to the
same partition, which guarantees they are processed in order by the same stream thread.

---

### `service/StoreQueryService.java`
**Package:** `org.example.service`

A Spring `@Service` that reads directly from the RocksDB state store using Kafka
Streams **Interactive Queries** — without going back through Kafka at all.

**How it gets access to the store:**

Spring Kafka's `StreamsBuilderFactoryBean` is the object that manages the
`KafkaStreams` instance. Injecting it gives access to the live streams instance:

```java
KafkaStreams streams = factory.getKafkaStreams();
ReadOnlyKeyValueStore<String, UserStats> store = streams.store(
    StoreQueryParameters.fromNameAndType("user-stats-store", QueryableStoreTypes.keyValueStore())
);
```

| Method | Description |
|--------|-------------|
| `getForUser(userId)` | Point lookup — `O(log n)` RocksDB read for one user. Returns `Optional.empty()` if the user has no purchases yet |
| `getAll()` | Full scan — iterates every key in the store. Fine for a small number of users; becomes slow at millions of keys |
| `logStore()` | `@Scheduled(fixedDelay=5000)` — prints the entire store to the console every 5 seconds so you can watch aggregates update in real time |

**The `withStore()` helper** checks that `KafkaStreams` is in `RUNNING` state before
querying. If the app just started, is rebalancing, or is restoring from the changelog,
the store is unavailable and `withStore()` throws `IllegalStateException`. The REST
controller catches this and returns HTTP 503.

---

### `web/StatsController.java`
**Package:** `org.example.web`

A Spring `@RestController` that exposes the application's two capabilities — producing
events and querying the store — as HTTP endpoints.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/stats` | Returns all users' stats as a JSON map from the RocksDB store |
| `GET` | `/api/stats/{userId}` | Returns one user's stats. `404` if not found, `503` if Streams not ready |
| `POST` | `/api/purchases` | Accepts a `Purchase` JSON body and sends it to Kafka |
| `POST` | `/api/purchases/random` | Generates and sends a random purchase, returns it |

**503 handling:** If Kafka Streams is still starting up or rebalancing, the store cannot
be queried. The controller catches `IllegalStateException` from `StoreQueryService` and
returns HTTP 503 Service Unavailable with the error message as the body.

---

### `StreamsTopologyTest.java`
**Package:** `org.example` (test)

A JUnit 5 unit test that verifies the aggregation logic without needing a running Kafka
broker. It uses Kafka Streams' **`TopologyTestDriver`** — an in-process test harness
that simulates the full topology.

**How it works:**

```java
// 1. Build the topology the same way Spring would, but without Spring context
StreamsBuilder builder = new StreamsBuilder();
new PurchaseTopologyConfig().purchaseAggregationStream(builder);

// 2. Wrap it in the test driver
driver = new TopologyTestDriver(builder.build(), props);

// 3. Push test records in
purchases.pipeInput("alice", new Purchase("alice", "coffee", 5.00));
purchases.pipeInput("alice", new Purchase("alice", "book",   15.00));

// 4. Read the state store directly and assert
UserStats alice = driver.getKeyValueStore("user-stats-store").get("alice");
assertEquals(2,     alice.count());
assertEquals(20.00, alice.total(), 0.001);
```

The test is completely standalone — no Spring context, no Kafka, no Docker. It runs in
milliseconds and verifies that `UserStats.add()` and the aggregation pipeline are
correct.

---

## Configuration — `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```
The broker address. Defaults to `localhost:9092` for local development. Overridden by
an environment variable in Docker and Kubernetes.

```yaml
    producer:
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        spring.json.add.type.headers: false
```
Uses Spring Kafka's Jackson-based serializer for the producer. `add.type.headers: false`
means no Java class name is embedded in the Kafka message headers — the consumer
(Streams topology) already knows the type statically via `JsonSerde`.

```yaml
    streams:
      application-id: kstreams-demo
      state-dir: ${SPRING_KAFKA_STREAMS_STATE_DIR:./kafka-streams-state}
      properties:
        commit.interval.ms: 1000
        application.server: ${SPRING_KAFKA_STREAMS_PROPERTIES_APPLICATION_SERVER:}
```
- `application-id` — the Kafka consumer group ID for this Streams app. All instances
  of the app share this ID, which is how Kafka knows to distribute partitions among them.
- `state-dir` — where RocksDB files are written on disk. Overridden in K8s to point to
  a PersistentVolumeClaim (PVC) so data survives pod restarts.
- `commit.interval.ms` — how often Kafka Streams flushes the RocksDB cache and commits
  offsets back to Kafka. 1000ms = 1 second.
- `application.server` — this pod's hostname:port. Kafka Streams broadcasts this to all
  other instances so they know where to route Interactive Query requests for keys owned
  by this instance. Set via Kubernetes Downward API.

```yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```
On `SIGTERM` (pod termination, `Ctrl+C`), Spring waits up to 30 seconds for Kafka
Streams to finish processing in-flight records, commit offsets, and close RocksDB
cleanly before the process exits. This prevents data loss and corruption.

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true
```
Enables `/actuator/health/liveness` and `/actuator/health/readiness` endpoints. These
are polled by Kubernetes to decide whether the pod is healthy and whether it should
receive traffic.

---

## Dockerfile — Multi-Stage Build

The Dockerfile has **3 stages** to produce the smallest and most efficient image:

```
Stage 1 (deps):
  Base image: maven:3.9-eclipse-temurin-17-alpine
  Action:     COPY pom.xml → mvn dependency:go-offline
  Purpose:    Downloads all Maven dependencies into a Docker layer.
              This layer is cached and NOT rebuilt when only source code changes —
              only when pom.xml changes. Saves minutes on each rebuild.

Stage 2 (builder):
  Base image: inherits from stage 1 (has all dependencies)
  Action:     COPY src → mvn package -DskipTests
  Purpose:    Compiles the code and produces the fat JAR.
              Rebuilt on every source code change.

Stage 3 (runtime):
  Base image: eclipse-temurin:17-jre-alpine  ← JRE only, no JDK, no Maven
  Action:     COPY the JAR from stage 2 → run it
  Purpose:    The final image. Contains only what is needed to run the app.
              No build tools, no source code, no Maven cache.
              Smaller image = faster pull, smaller attack surface.
```

**JVM flags:**
- `-XX:+UseContainerSupport` — makes the JVM read memory limits from cgroup (Docker/K8s
  resource limits) instead of the host machine's total RAM
- `-XX:MaxRAMPercentage=65.0` — JVM heap gets 65% of the container's memory limit.
  The remaining 35% is left for RocksDB's native off-heap memory and the OS

**Security:** A non-root user (`spring`) is created and the process runs as that user.
Running as root inside a container is a security risk.

---

## docker-compose.yml

Runs the full stack locally in containers — no local Kafka installation needed.

**Three services, in dependency order:**

```
kafka (broker) → kafka-init (topic creation) → app (Spring Boot)
```

| Service | Image | Role |
|---------|-------|------|
| `kafka` | `bitnami/kafka:3.9` | Single-broker Kafka in KRaft mode. Healthcheck polls `kafka-topics.sh` so downstream services wait until the broker is actually ready |
| `kafka-init` | `bitnami/kafka:3.9` | Runs once, creates the `purchases` and `user-stats` topics, then exits. Uses `depends_on: condition: service_healthy` to wait for the broker |
| `app` | Built from `Dockerfile` | The Spring Boot app. Only starts after `kafka-init` completes successfully. RocksDB data is written to a named Docker volume (`rocksdb-data`) so it persists across `docker compose restart` |

**Run it:**
```bash
docker compose up --build
curl -X POST http://localhost:8080/api/purchases/random
curl http://localhost:8080/api/stats
```

---

## Kubernetes Manifests (`k8s/`)

### Why a StatefulSet and not a Deployment?

A `Deployment` creates pods with random names and no guaranteed identity. A `StatefulSet`
gives each pod a stable, predictable name (`kstreams-demo-0`, `kstreams-demo-1`, etc.)
and its own PersistentVolumeClaim. For Kafka Streams this matters for three reasons:

1. **Partition ownership is sticky.** Pod-0 always gets the same Kafka partitions after
   a restart, meaning it finds its local RocksDB state intact and resumes processing
   without replaying the changelog.

2. **Interactive Queries routing.** When a request for `alice`'s stats hits pod-1 but
   alice's data is on pod-0, pod-1 needs to forward the request to pod-0. This requires
   knowing pod-0's address, which is only predictable with a StatefulSet and a headless
   service.

3. **Per-pod PVCs.** `volumeClaimTemplates` gives each pod its own disk. Pod-0's RocksDB
   is never mixed with pod-1's.

---

### `k8s/namespace.yml`

Creates a Kubernetes **namespace** called `kafka-demo`. Namespaces are logical isolation
boundaries within a cluster — all resources for this project live in `kafka-demo` and
are separated from other applications.

---

### `k8s/kafka.yml`

A single-broker Kafka `StatefulSet` for use inside Kubernetes during development.

> **For production**, use the [Strimzi operator](https://strimzi.io) instead. Strimzi
> manages multi-broker clusters, TLS, authentication, rolling upgrades, topic operators,
> and more. A hand-written StatefulSet is not sufficient for production Kafka.

The broker runs in **KRaft mode** (no ZooKeeper). The `KAFKA_CFG_ADVERTISED_LISTENERS`
is set to the K8s service DNS name (`kafka-svc.kafka-demo.svc.cluster.local:9092`) so
the Spring Boot app and other pods can connect to it by name.

---

### `k8s/configmap.yml`

A `ConfigMap` holds environment variables that are injected into the app pods. Keeping
config outside the image means you can change Kafka addresses, thread counts, or log
levels without rebuilding the Docker image.

| Key | Value | Effect |
|-----|-------|--------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `kafka-svc...:9092` | Points the app to the in-cluster Kafka service |
| `SPRING_KAFKA_STREAMS_STATE_DIR` | `/data/kafka-streams-state` | RocksDB writes to the PVC mounted at `/data` |
| `...NUM_STREAM_THREADS` | `3` | One thread per partition — full parallelism |
| `...DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER` | `LogAndContinueExceptionHandler` | On a bad/corrupt message, log the error and skip the record instead of crashing the stream thread |

---

### `k8s/statefulset.yml`

The core deployment manifest. Key decisions explained:

**`terminationGracePeriodSeconds: 45`**
When K8s kills a pod (rolling update, scale-down), it sends `SIGTERM` and waits 45
seconds before force-killing. Spring's graceful shutdown uses 30 of those seconds to
flush and close cleanly. The 15-second gap is the safety margin.

**`application.server` via Downward API**
```yaml
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name          # e.g. "kstreams-demo-1"
- name: SPRING_KAFKA_STREAMS_PROPERTIES_APPLICATION_SERVER
  value: "$(POD_NAME).kstreams-headless.kafka-demo.svc.cluster.local:8080"
```
The pod injects its own name into an env var, which becomes its Kafka Streams
`application.server`. Other pods learn this address from Kafka Streams cluster metadata
and can forward Interactive Query requests directly to the right pod.

**Liveness vs Readiness probes:**
| Probe | Endpoint | Meaning |
|-------|----------|---------|
| Liveness | `/actuator/health/liveness` | Is the JVM alive? If this fails, K8s restarts the pod |
| Readiness | `/actuator/health/readiness` | Is Streams in RUNNING state and ready to serve traffic? If this fails, K8s removes the pod from the Service's load balancer until it recovers |

---

### `k8s/service.yml`

Two services are defined:

**`kstreams-headless` (Headless — `clusterIP: None`)**
A headless service does not load-balance. Instead it creates individual DNS A records
for each pod:
```
kstreams-demo-0.kstreams-headless.kafka-demo.svc.cluster.local → pod-0 IP
kstreams-demo-1.kstreams-headless.kafka-demo.svc.cluster.local → pod-1 IP
kstreams-demo-2.kstreams-headless.kafka-demo.svc.cluster.local → pod-2 IP
```
This is required by the StatefulSet and is what makes cross-pod Interactive Query
routing possible.

**`kstreams-svc` (ClusterIP)**
A regular load-balanced service. Any REST API request from inside the cluster hits
this service and is forwarded to one of the pods. For external access, change the type
to `LoadBalancer` or put an Ingress in front of it.

---

## How the Pieces Fit Together at Runtime

```
┌──────────────────────────────────────────────────────────────────┐
│  Kubernetes cluster                                              │
│                                                                  │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐         │
│  │  Pod-0       │   │  Pod-1       │   │  Pod-2       │         │
│  │  partition 0 │   │  partition 1 │   │  partition 2 │         │
│  │  RocksDB p0  │   │  RocksDB p1  │   │  RocksDB p2  │         │
│  │  (PVC-0)     │   │  (PVC-1)     │   │  (PVC-2)     │         │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘         │
│         │                  │                  │                  │
│         └──────────────────┴──────────────────┘                  │
│                            │                                     │
│                    kstreams-svc (ClusterIP)                      │
│                    POST /api/purchases/random                    │
│                    GET  /api/stats                               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │  Kafka Broker (kafka-svc)                               │     │
│  │   purchases topic           (3 partitions)              │     │
│  │   user-stats topic          (3 partitions)              │     │
│  │   user-stats-store-changelog (3 partitions — backup)    │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

**What happens when you call `POST /api/purchases/random`:**
1. `StatsController` calls `PurchaseProducerService.sendRandom()`
2. `KafkaTemplate` serialises the `Purchase` to JSON and sends it to the `purchases`
   topic, keyed by `userId`
3. Kafka routes it to the partition for that `userId` (e.g. partition 1)
4. Pod-1's embedded Kafka Streams consumer picks it up
5. `PurchaseTopologyConfig` runs `aggregate()` — calls `UserStats.add()` with the new purchase
6. The updated `UserStats` is written to Pod-1's local RocksDB and to the changelog topic

**What happens when you call `GET /api/stats/alice`:**
1. `StatsController` calls `StoreQueryService.getForUser("alice")`
2. `StoreQueryService` opens the local RocksDB store and does a key lookup for `"alice"`
3. The result is returned instantly — no Kafka involved
4. If alice's data is on a different pod (in a multi-node setup), the request would need
   to be forwarded using `streams.queryMetadataForKey()` — this is the next step for
   full production readiness

---

## Running the Project

### Option 1 — Local (Kafka installed via Homebrew)
```bash
brew services start kafka
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic purchases  --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-stats --partitions 3 --replication-factor 1
mvn spring-boot:run
```

### Option 2 — Docker Compose (no local Kafka needed)
```bash
docker compose up --build
```

### Option 3 — Kubernetes (minikube)
```bash
eval $(minikube docker-env)
docker build -t kstreams-demo:latest .
kubectl apply -f k8s/namespace.yml
kubectl apply -f k8s/kafka.yml
kubectl apply -f k8s/configmap.yml
kubectl apply -f k8s/service.yml
kubectl apply -f k8s/statefulset.yml
kubectl port-forward -n kafka-demo svc/kstreams-svc 8080:8080
```

### REST API
```bash
# Produce a random purchase
curl -X POST http://localhost:8080/api/purchases/random

# Produce a specific purchase
curl -X POST http://localhost:8080/api/purchases \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","item":"book","amount":29.99}'

# Query all users
curl http://localhost:8080/api/stats

# Query one user
curl http://localhost:8080/api/stats/alice

# Health probes
curl http://localhost:8080/actuator/health/liveness
curl http://localhost:8080/actuator/health/readiness
```

### Run Tests (no broker needed)
```bash
mvn test
```

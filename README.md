# KStreamsDemo

A **Kafka Streams + Spring Boot** playground demonstrating KTables, RocksDB state
stores, and Interactive Queries.

Purchase events flow into a Kafka topic, get aggregated per user in real time, and
the results are stored in RocksDB — queryable instantly via REST without touching Kafka.

```
POST /api/purchases/random
        │
        ▼
  purchases topic  ──►  groupByKey  ──►  aggregate  ──►  RocksDB ("user-stats-store")
                                                               │
                                                               ▼
                                                       GET /api/stats
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java | 17+ | `brew install openjdk@17` |
| Maven | 3.9+ | `brew install maven` |
| Kafka | 3.9+ | `brew install kafka` |
| Docker | any | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |

---

## Option 1 — Run Locally (Kafka on your Mac)

### Step 1 — Start Kafka

```bash
brew services start kafka
```

### Step 2 — Create the topics

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic purchases  --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic user-stats --partitions 3 --replication-factor 1
```

### Step 3 — Start the app

```bash
mvn spring-boot:run
```

Wait until you see:

```
Started KStreamsDemoApplication in X seconds
```

### Step 4 — Produce events and query the store

Open a second terminal:

```bash
# Produce one random purchase
curl -X POST http://localhost:8080/api/purchases/random

# Produce 10 random purchases
for i in {1..10}; do curl -s -X POST http://localhost:8080/api/purchases/random; echo; done

# Query all users from the RocksDB store
curl http://localhost:8080/api/stats

# Query a specific user
curl http://localhost:8080/api/stats/alice

# Produce a specific purchase
curl -X POST http://localhost:8080/api/purchases \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","item":"book","amount":29.99}'
```

The app also logs the store contents automatically every 5 seconds in the first terminal.

---

## Option 2 — Run with Docker Compose (no local Kafka needed)

```bash
docker compose up --build
```

This starts Kafka, creates the topics, and starts the app — all in one command.
Once running, use the same `curl` commands from Option 1.

```bash
# Stop everything
docker compose down
```

---

## Option 3 — Run on Kubernetes (minikube)

### Step 1 — Build the image into minikube

```bash
eval $(minikube docker-env)
docker build -t kstreams-demo:latest .
```

### Step 2 — Deploy

```bash
kubectl apply -f k8s/namespace.yml
kubectl apply -f k8s/kafka.yml
kubectl apply -f k8s/configmap.yml
kubectl apply -f k8s/service.yml
kubectl apply -f k8s/statefulset.yml
```

### Step 3 — Wait for pods to be ready

```bash
kubectl get pods -n kafka-demo -w
```

All pods should show `Running` and `READY 1/1`.

### Step 4 — Forward the port and test

```bash
kubectl port-forward -n kafka-demo svc/kstreams-svc 8080:8080
curl -X POST http://localhost:8080/api/purchases/random
curl http://localhost:8080/api/stats
```

---

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/purchases/random` | Produce a random purchase event |
| `POST` | `/api/purchases` | Produce a specific purchase (JSON body) |
| `GET` | `/api/stats` | All users' aggregated stats from RocksDB |
| `GET` | `/api/stats/{userId}` | One user's stats (`404` if not found, `503` if Streams not ready) |
| `GET` | `/actuator/health` | App health |
| `GET` | `/actuator/health/liveness` | K8s liveness probe |
| `GET` | `/actuator/health/readiness` | K8s readiness probe |

---

## Run Tests (no Kafka needed)

```bash
mvn test
```

Uses `TopologyTestDriver` — runs the full topology in-process with no broker.

---

## Inspect RocksDB on disk

```bash
find kafka-streams-state -type f | sort
```

You will see RocksDB's own files (`*.log`, `*.sst`, `MANIFEST`, `CURRENT`, `OPTIONS`)
organised per partition under `kafka-streams-state/kstreams-demo/`.

**To test fault tolerance:** stop the app, delete the state directory, restart.
The store rebuilds automatically from the Kafka changelog topic.

```bash
rm -rf kafka-streams-state/
mvn spring-boot:run   # store restored from changelog
curl http://localhost:8080/api/stats   # same data as before
```

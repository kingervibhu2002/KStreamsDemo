# KStreamsDemo

A small playground for **Kafka Streams**, **KTables**, and **RocksDB** state stores.

The demo aggregates purchase events per user into a materialized KTable that is
backed by RocksDB on disk, and reads that store back with Interactive Queries.

```
purchases topic ──stream──> groupByKey ──aggregate──> KTable (RocksDB store "user-stats-store")
   key=userId                                              │
   value=Purchase(item, amount)                            └──> user-stats topic (changelog)
```

Per user we keep a running `UserStats(count, total, maxAmount)` — see
[`UserStats`](src/main/java/org/example/model/UserStats.java) for the
initializer/aggregator logic and [`StreamsTopology`](src/main/java/org/example/StreamsTopology.java)
for the topology.

## Requirements

- Java 17+ (you have 17)
- Maven (you have 3.9)
- A local Kafka broker on `localhost:9092`

## 1. Install & start Kafka locally (KRaft, no ZooKeeper)

Kafka 4.x runs in KRaft mode — no ZooKeeper needed.

```bash
brew install kafka
```

Homebrew can run it as a service:

```bash
brew services start kafka        # starts broker on localhost:9092
```

Or run it in the foreground from the install dir (adjust the path to your
Homebrew prefix — `brew --prefix kafka`):

```bash
KAFKA=$(brew --prefix kafka)
# Format storage once (first time only):
KAFKA_CLUSTER_ID=$("$KAFKA"/libexec/bin/kafka-storage.sh random-uuid)
"$KAFKA"/libexec/bin/kafka-storage.sh format \
    -t "$KAFKA_CLUSTER_ID" \
    -c "$KAFKA"/libexec/config/server.properties
# Start the broker:
"$KAFKA"/libexec/bin/kafka-server-start.sh "$KAFKA"/libexec/config/server.properties
```

> With `brew services start kafka` the storage is formatted for you.

## 2. Create the topics

```bash
KAFKA=$(brew --prefix kafka)
"$KAFKA"/libexec/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic purchases  --partitions 3 --replication-factor 1
"$KAFKA"/libexec/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic user-stats --partitions 3 --replication-factor 1
```

(Streams would auto-create its internal changelog/repartition topics, but the
input/output topics are yours to create.)

## 3. Run the Streams app

```bash
mvn exec:java -Dexec.mainClass=org.example.StreamsApp
```

It starts the topology and every few seconds prints the current contents of the
RocksDB store via Interactive Queries:

```
---- user-stats-store @ 14:02:11 ----
  alice    count=3   total=  420.50 avg= 140.17 max= 300.00
  bob      count=1   total= 1200.00 avg=1200.00 max=1200.00
```

## 4. Produce some purchases (second terminal)

```bash
mvn exec:java -Dexec.mainClass=org.example.ProducerApp -Dexec.args="30"
# -Dexec.args="0" produces endlessly until Ctrl+C
```

Watch the numbers in the Streams app terminal update as events flow in.

## 5. Inspect RocksDB on disk

The state store is persisted under `./kafka-streams-state/kstreams-demo/`:

```bash
find kafka-streams-state -maxdepth 4 -type d
ls kafka-streams-state/kstreams-demo/*/rocksdb/user-stats-store
```

You'll see RocksDB's own files (`*.sst`, `LOG`, `CURRENT`, `MANIFEST-*`, ...).
Because the KTable is also backed by a compacted changelog topic in Kafka, the
store is rebuilt automatically if you delete this directory and restart.

## Watch the output topic directly (optional)

```bash
KAFKA=$(brew --prefix kafka)
"$KAFKA"/libexec/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic user-stats --property print.key=true --from-beginning
```

## Test without a broker

[`StreamsTopologyTest`](src/test/java/org/example/StreamsTopologyTest.java) runs
the whole topology in-process with `TopologyTestDriver` — no Kafka required:

```bash
mvn test
```

## Things to try next

- Change the aggregation in `UserStats` (e.g. track min, or per-item breakdown).
- Add a **windowed** aggregation (`.windowedBy(TimeWindows.ofSizeWithNoGrace(...))`)
  to see windowed RocksDB stores.
- Add a **KStream–KTable join** (e.g. enrich purchases with a users KTable).
- Delete `kafka-streams-state/` while stopped and restart to watch the store
  restore from the changelog.

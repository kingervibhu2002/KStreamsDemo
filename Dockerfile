# ── Stage 1: resolve & cache Maven dependencies ──────────────────────────────
# Copying pom.xml first and running dependency:go-offline means this layer is
# only rebuilt when pom.xml changes, not on every source change.
FROM maven:3.9-eclipse-temurin-17 AS deps
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B -q

# ── Stage 2: compile & package ───────────────────────────────────────────────
FROM deps AS builder
COPY src ./src
RUN mvn package -DskipTests -B -q

# ── Stage 3: minimal runtime image ───────────────────────────────────────────
# Debian-based (not Alpine) — RocksDB's native JNI lib requires libstdc++.so.6
# which is present in glibc-based images but absent in Alpine (musl libc).
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Non-root user for security hardening.
# Create /data here (as root) and chown it so the spring user can write the
# RocksDB state dir when Docker mounts a named volume at /data.
RUN groupadd -r spring && useradd -r -g spring spring && \
    mkdir -p /data && chown spring:spring /data
USER spring

COPY --from=builder /build/target/*.jar app.jar

EXPOSE 8080

# MaxRAMPercentage=65: leaves ~35% for RocksDB off-heap native memory + OS.
# UseContainerSupport (default in Java 11+) makes the JVM respect cgroup limits
# instead of reading the host's total RAM.
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=65.0", \
  "-jar", "app.jar"]

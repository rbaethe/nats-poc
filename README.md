#### 1. Build and start jetstream client

5.1 Build jetstream client

```

mvn clean package

```

5.2 Start jetstream client

```

java -jar ./target/jetstream-client-0.0.1-SNAPSHOT.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000

```
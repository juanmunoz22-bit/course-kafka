## Topic Management via CLI

### Create Topic

```bash
kafka-topics --create --topic people --partitions 3 --replication-factor 3 --bootstrap-server broker0:29092
```

### List Topics

```bash
kafka-topics --list --bootstrap-server broker0:29092,broker1:29093,broker2:29093
```

### Describe Topic

```bash
kafka-topics --describe --topic people --bootstrap-server broker0:29092
```


### Delete Topic

```bash
kafka-topics --delete --topic people --bootstrap-server broker0:29092
```

### Create Topic with different retention

```bash
kafka-topics --create --topic experiments --bootstrap-server broker0:29092 --config retention.ms=60000
```

### Change Retention of a Topic

```bash
kafka-configs --alter --entity-type topics --entity-name people --add-config retention.ms=60000 --bootstrap-server broker0:29092
```

### Create Compacted Topic

```bash
kafka-topics --create --topic experiments.latest --bootstrap-server broker0:29092 --config cleanup.policy=compact
```

### Open the Kafka Console Producer

```bash
kafka-console-producer --topic people --broker-list broker0:29092
```

### List Consumer Groups

```bash
kafka-consumer-groups --bootstrap-server broker0:29092 --list
```

### Reset Offsets of a Consumer Group

```bash
kafka-consumer-groups --bootstrap-server broker0:29092 --reset-offsets --to-offset 5 --group people.advanced.python.grp-0 --topic people.advanced.python:0
```
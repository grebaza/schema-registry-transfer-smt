Schema Registry Transfer SMT
============================
[ ![Download](https://api.bintray.com/packages/cricket007/maven-releases/schema-registry-transfer-smt/images/download.svg) ](https://bintray.com/cricket007/maven-releases/schema-registry-transfer-smt/_latestVersion)&nbsp;
[![CircleCI](https://circleci.com/gh/cricket007/schema-registry-transfer-smt.svg?style=svg)](https://circleci.com/gh/cricket007/schema-registry-transfer-smt)

A [Kafka Connect Single Message Transformation (SMT)][smt] that reads the serialized [wire format header][wire-format] of Confluent's `KafkaAvroSerializer`, performs a lookup against a source [Confluent Schema Registry][schema-registry] for the ID in the message, and registers that schema into a destination Registry for that topic/subject under a new ID.

To be used where it is not feasible to make the destination Schema Registry as a follower to the source Registry, or when migrating topics to a new cluster.

> _Requires that the Kafka Connect tasks can reach both Schema Registries._

This transform doesn't mirror the contents of the `_schemas` topic, so therefore each registry can be completely isolated from one another. As a side-effect of this, the subject configurations that might be applied to the `/config` endpoint in the source registry are not copied to the destination. In other words, you might get schema registration errors if using differing compatibility levels on the registries. Just a heads-up.

Example Kafka Connectors where this could be applied.

- [Comcast/MirrorTool-for-Kafka-Connect](https://github.com/Comcast/MirrorTool-for-Kafka-Connect) - Code was tested with this first, and verified that the topic-renaming logic of this connector worked fine with this SMT.
- [Salesforce/mirus](https://github.com/salesforce/mirus)
- [Confluent Replicator](https://docs.confluent.io/current/connect/kafka-connect-replicator/index.html) - While this already can copy the schema, we observed it is only possible via the `AvroConverter`, which must first parse the entire message into a Kafka Connect `Struct` object. Thus, the class here is considered a "shallow" copier — it only inspects [the first 5 bytes][wire-format] of the keys and values for the schema ids.
- [KIP-382 (MirrorMaker 2.0)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0) - Still open at the time of writing.


## Installation

1. Edit the Kafka Connect worker properties file on each worker to include a new directory. For example, `/opt/kafka-connect/plugins`

```sh
plugin.path=/usr/share/java,/opt/kafka-connect/plugins
```

2. Build this project

```sh
./mvnw clean package
```

3. Copy the JAR from `target` to all Kafka Connect workers under a directory set by `plugin.path`

4. (Re)start Kafka Connect processes

## Usage

Standalone Kafka Connect configuration section

```properties
# Requires that records are entirely byte-arrays. These can go in the worker or connector configuration.
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# Setup the SMT
transforms=AvroSchemaTransfer

transforms.AvroSchemaTransfer.type=cricket.jmoore.kafka.connect.transforms.SchemaRegistryTransfer
transforms.AvroSchemaTransfer.src.schema.registry.url=http://schema-registry-1:8081
transforms.AvroSchemaTransfer.dest.schema.registry.url=http://schema-registry-2:8081
```

Distributed Kafka Connect configuration section

```json
"config" : {
    ...

    "__comment": "Requires that records are entirely byte-arrays. These can go in the worker or connector configuration.",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",

    "__comment": "Setup the SMT",
    "transforms": "AvroSchemaTransfer",

    "transforms.AvroSchemaTransfer.type": "cricket.jmoore.kafka.connect.transforms.SchemaRegistryTransfer",
    "transforms.AvroSchemaTransfer.src.schema.registry.url": "http://schema-registry-1:8081",
    "transforms.AvroSchemaTransfer.dest.schema.registry.url": "http://schema-registry-2:8081"
}
```

## Advanced Configuration

Configuration Parameter | Default | Description
----------------------- | ------- | -----------
**transfer.message.keys** | true | Indicates whether Avro schemas from message keys in source records should be copied to the destination Registry.
**include.message.headers** | true | Indicates whether message headers from source records should be preserved after the transform.
**schema.capacity** | 100 | Capacity of schemas that can be cached in each `CachedSchemaRegistryClient`.
**dest.compatibility.type** | "" | Compatibility type for topics on destination Registry. Look [here](https://docs.confluent.io/platform/current/schema-registry/avro.html#compatibility-types) for options.
**key.subject.name.strategy** | io.confluent.kafka.serializers.subject.TopicNameStrategy | Key subject name strategy class on destination Registry.
**value.subject.name.strategy** | io.confluent.kafka.serializers.subject.TopicNameStrategy | Value subject name strategy class on destination Registry.
**(src\|dest).schema.registry.url** | | URL for source and destination Registries.
**(src\|dest).ssl.truststore.location** | | The location of the trust store file.
**(src\|dest).ssl.truststore.password** | | The password for the trust store file. If a password is not set, trust store file configured will still be used, but integrity checking is disabled. Trust store password is not supported for PEM format.
**(src\|dest).ssl.keystore.location** | | The location of the key store file. This is optional for client and can be used for two-way authentication for client.
**(src\|dest).ssl.keystore.password** | | The store password for the key store file. This is optional for client and only needed if 'ssl.keystore.location' is configured. Key store password is not supported for PEM format.
**(src\|dest).ssl.key.password** | | The password of the private key in the key store file or the PEM key specified in `ssl.keystore.key`. This is required for clients only if two-way authentication is configured.

## Embedded Schema Registry Client Configuration

Schema Registry Transfer SMT passes some properties prefixed by either `src.` or `dest.`
through to its embedded schema registry clients, after stripping away `src.` or `dest.`
prefix used to disambiguate which client is to receive which configuration value.

Properties prefixed by `src.` are passed through to the source consumer's schema registry
client.  Properties prefixed by `dest.` are passed through to the target producer's schema
registry client.

Configuration Parameter | Default | Description
----------------------- | ------- | -----------
<b>(src\|dest).basic.auth.credentials.source</b> | URL | Specify how to pick credentials for Basic Auth header. Supported values are `URL`, `USER_INFO` and `SASL_INHERIT`
<b>(src\|dest).basic.auth.user.info</b> |  | Specify credentials for Basic Auth in form of `{username}:{password}` when source is `USER_INFO`

## Subject Renaming

Renaming of a subject can be done with the `RegexRouter` Transform **before** this one.

Example Configuration

```properties
transforms=TopicRename,AvroSchemaTransfer

transforms.TopicRename.type=org.apache.kafka.connect.transforms.RegexRouter
transforms.TopicRename.regex=(.*)
transforms.TopicRename.replacement=replica.$1

transforms.AvroSchemaTransfer.type=...
```

<!-- Links -->
  [smt]: https://docs.confluent.io/current/connect/concepts.html#connect-transforms
  [schema-registry]: https://docs.confluent.io/current/schema-registry/docs/index.html
  [wire-format]: https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format

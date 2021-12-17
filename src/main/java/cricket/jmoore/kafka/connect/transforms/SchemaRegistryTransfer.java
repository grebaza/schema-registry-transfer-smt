/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

@SuppressWarnings("unused")
public class SchemaRegistryTransfer<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryTransfer.class);

  public static final ConfigDef CONFIG_DEF;

  private static int callNumber = 0;
  private static final byte MAGIC_BYTE = (byte) 0x0;
  // wire-format is magic byte + an integer, then data
  private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

  public static final String OVERVIEW_DOC =
      "Inspect the Confluent KafkaSchemaSerializer's wire-format header to copy schemas from one Schema Registry to another.";
  public static final String SCHEMA_CAPACITY_CONFIG_DOC =
      "The maximum amount of schemas to be stored for each Schema Registry client.";
  public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;

  // source schema-registry configuration
  public static final String SRC_PREAMBLE = "For source consumer's schema registry, ";
  public static final String SRC_SCHEMA_REGISTRY_CONFIG_DOC =
      "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
  public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC =
      SRC_PREAMBLE + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
  public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
  public static final String SRC_USER_INFO_CONFIG_DOC =
      SRC_PREAMBLE + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
  public static final String SRC_USER_INFO_CONFIG_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

  // destination schema-registry configuration
  public static final String DEST_PREAMBLE = "For target producer's schema registry, ";
  public static final String DEST_SCHEMA_REGISTRY_CONFIG_DOC =
      "A list of addresses for the Schema Registry to copy to. The producer's Schema Registry.";
  public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
  public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
  public static final String DEST_USER_INFO_CONFIG_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
  public static final String DEST_USER_INFO_CONFIG_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;
  public static final String DEST_AUTO_REGISTER_SCHEMAS_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS_DOC;
  public static final String DEST_AUTO_REGISTER_SCHEMAS_DEFAULT =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS_DEFAULT;
  public static final String DEST_USE_LATEST_VERSION_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION_DOC;
  public static final String DEST_USE_LATEST_VERSION_DEFAULT =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION_DEFAULT;
  public static final String DEST_LATEST_COMPATIBILITY_STRICT_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT_DOC;
  public static final String DEST_LATEST_COMPATIBILITY_STRICT_DEFAULT =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT_DEFAULT;

  public static final String TRANSFER_KEYS_CONFIG_DOC =
      "Whether or not to copy message key schemas between registries.";
  public static final Boolean TRANSFER_KEYS_CONFIG_DEFAULT = true;
  public static final String INCLUDE_HEADERS_CONFIG_DOC =
      "Whether or not to preserve the Kafka Connect Record headers.";
  public static final Boolean INCLUDE_HEADERS_CONFIG_DEFAULT = true;

  private static final int KEY_VALUE_MIN_LEN = 5;
  private CachedSchemaRegistryClient sourceSchemaRegistryClient;
  private CachedSchemaRegistryClient destSchemaRegistryClient;
  private SubjectNameStrategy subjectNameStrategy;
  private boolean transferKeys, includeHeaders;

  // caches from the source registry to the destination registry
  private Cache<Integer, SchemaAndId> schemaCache;

  public SchemaRegistryTransfer() {}

  static {
    CONFIG_DEF =
        (new ConfigDef())
            .define(
                ConfigName.SRC_SCHEMA_REGISTRY_URL,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                new NonEmptyListValidator(),
                ConfigDef.Importance.HIGH,
                SRC_SCHEMA_REGISTRY_CONFIG_DOC)
            .define(
                ConfigName.DEST_SCHEMA_REGISTRY_URL,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                new NonEmptyListValidator(),
                ConfigDef.Importance.HIGH,
                DEST_SCHEMA_REGISTRY_CONFIG_DOC)
            .define(
                ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE,
                ConfigDef.Type.STRING,
                SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
            .define(
                ConfigName.SRC_USER_INFO,
                ConfigDef.Type.PASSWORD,
                SRC_USER_INFO_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                SRC_USER_INFO_CONFIG_DOC)
            .define(
                ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE,
                ConfigDef.Type.STRING,
                DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
            .define(
                ConfigName.DEST_USER_INFO,
                ConfigDef.Type.PASSWORD,
                DEST_USER_INFO_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DEST_USER_INFO_CONFIG_DOC)
            .define(
                ConfigName.DEST_AUTO_REGISTER_SCHEMAS,
                ConfigDef.Type.BOOLEAN,
                DEST_AUTO_REGISTER_SCHEMAS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DEST_AUTO_REGISTER_SCHEMAS_DOC)
            .define(
                ConfigName.DEST_USE_LATEST_VERSION,
                ConfigDef.Type.BOOLEAN,
                DEST_USE_LATEST_VERSION_DEFAULT,
                ConfigDef.Importance.LOW,
                DEST_USE_LATEST_VERSION_DOC)
            .define(
                ConfigName.DEST_LATEST_COMPATIBILITY_STRICT,
                ConfigDef.Type.BOOLEAN,
                DEST_LATEST_COMPATIBILITY_STRICT_DEFAULT,
                ConfigDef.Importance.LOW,
                DEST_LATEST_COMPATIBILITY_STRICT_DOC)
            .define(
                ConfigName.SCHEMA_CAPACITY,
                ConfigDef.Type.INT,
                SCHEMA_CAPACITY_CONFIG_DEFAULT,
                ConfigDef.Importance.LOW,
                SCHEMA_CAPACITY_CONFIG_DOC)
            .define(
                ConfigName.TRANSFER_KEYS,
                ConfigDef.Type.BOOLEAN,
                TRANSFER_KEYS_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                TRANSFER_KEYS_CONFIG_DOC)
            .define(
                ConfigName.INCLUDE_HEADERS,
                ConfigDef.Type.BOOLEAN,
                INCLUDE_HEADERS_CONFIG_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                INCLUDE_HEADERS_CONFIG_DOC);
    // todo: Other properties might be useful, e.g. the Subject Strategies
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    // source schema-registry config
    List<String> sourceUrls = config.getList(ConfigName.SRC_SCHEMA_REGISTRY_URL);
    final Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "SRC_" + config.getString(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE));
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
        config.getPassword(ConfigName.SRC_USER_INFO).value());

    // destination schema-registry config
    List<String> destUrls = config.getList(ConfigName.DEST_SCHEMA_REGISTRY_URL);
    final Map<String, String> destProps = new HashMap<>();
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "DEST_" + config.getString(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE));
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
        config.getPassword(ConfigName.DEST_USER_INFO).value());

    destProps.put(
        AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
        config.getString(ConfigName.DEST_AUTO_REGISTER_SCHEMAS));
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
        config.getString(ConfigName.DEST_USE_LATEST_VERSION));
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT,
        config.getString(ConfigName.DEST_LATEST_COMPATIBILITY_STRICT));

    Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

    this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
    this.sourceSchemaRegistryClient =
        new CachedSchemaRegistryClient(sourceUrls, schemaCapacity, sourceProps);
    this.destSchemaRegistryClient =
        new CachedSchemaRegistryClient(destUrls, schemaCapacity, destProps);

    this.transferKeys = config.getBoolean(ConfigName.TRANSFER_KEYS);
    this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

    // todo: Make the Strategy configurable, may be different for src and dest
    // Strategy for the -key and -value subjects
    this.subjectNameStrategy = new TopicNameStrategy();
  }

  @Override
  public R apply(R r) {
    final String topic = r.topic();

    // Transcribe the key's schema id
    final Object key = r.key();
    final Schema keySchema = r.keySchema();

    callNumber++;
    log.trace("Iteration {}\n==========", callNumber);
    Object updatedKey = key;
    if (transferKeys) {
      updatedKey = updateKeyValue(key, keySchema, topic, true);
    } else {
      log.info(
          "Skipping record key translation. {} has been to false. Keys will be passed as-is.",
          ConfigName.TRANSFER_KEYS);
    }

    // Transcribe the value's schema id
    final Object value = r.value();
    final Schema valueSchema = r.valueSchema();

    Object updatedValue = updateKeyValue(value, valueSchema, topic, false);

    return includeHeaders
        ? r.newRecord(
            topic,
            r.kafkaPartition(),
            keySchema,
            updatedKey,
            valueSchema,
            updatedValue,
            r.timestamp(),
            r.headers())
        : r.newRecord(
            topic,
            r.kafkaPartition(),
            keySchema,
            updatedKey,
            valueSchema,
            updatedValue,
            r.timestamp());
  }

  private Object updateKeyValue(Object object, Schema objectSchema, String topic, boolean isKey) {
    Optional<Integer> destSchemaId;
    Object updatedKeyValue = null;
    final String recordPart = isKey == true ? "key" : "value";

    if (ConnectSchemaUtil.isBytesSchema(objectSchema) || object instanceof byte[]) {
      if (object == null) {
        log.trace("Passing through null record {}.", recordPart);
      } else {
        byte[] objectAsBytes = (byte[]) object;
        int objectLength = objectAsBytes.length;
        if (objectLength <= KEY_VALUE_MIN_LEN) {
          throw new SerializationException(
              String.format(
                  "Unexpected byte[] length %d for Avro record %s.", objectLength, recordPart));
        }
        ByteBuffer b = ByteBuffer.wrap(objectAsBytes);
        log.trace("object dump: {}", Utils.bytesToHex(objectAsBytes));

        destSchemaId = copyAvroSchema(b, topic, true);
        b.putInt(
            1,
            destSchemaId.orElseThrow(
                () ->
                    new ConnectException(
                        String.format(
                            "Transform failed. Unable to update record schema id for %s",
                            recordPart))));
        updatedKeyValue = (Object) b.array();
      }
    } else {
      throw new ConnectException(
          String.format("Transform failed. Record %s does not have a byte[] schema.", recordPart));
    }

    return updatedKeyValue;
  }

  protected Optional<Integer> copyAvroSchema(ByteBuffer buffer, String topic, boolean isKey) {
    SchemaAndId schemaAndDestId;
    final String recordPart = isKey == true ? "key" : "value";

    if (buffer.get() == MAGIC_BYTE) {
      int sourceSchemaId = buffer.getInt();

      log.trace(
          "Looking up schema id {} in Schema Cache for record {}", sourceSchemaId, recordPart);
      schemaAndDestId = schemaCache.get(sourceSchemaId);
      if (schemaAndDestId != null) {
        log.trace(
            "Schema id {} has been seen before. Not registering with destination registry again for record {}",
            recordPart);
      } else { // cache miss
        log.trace(
            "Schema id {} has not been seen before for record {}", sourceSchemaId, recordPart);
        schemaAndDestId = new SchemaAndId();
        try {
          log.trace(
              "Looking up schema id {} in source registry for record {}",
              sourceSchemaId,
              recordPart);
          // Can't do getBySubjectAndId because that requires a Schema object for the strategy
          schemaAndDestId.schema =
              (AvroSchema) sourceSchemaRegistryClient.getSchemaById(sourceSchemaId);
        } catch (IOException | RestClientException e) {
          log.error(
              String.format(
                  "Unable to fetch source schema id %d for record %s", sourceSchemaId, recordPart),
              e);
          throw new ConnectException(e);
        }

        try {
          log.trace(
              "Registering schema {} to destination registry for record {}",
              schemaAndDestId.schema,
              recordPart);
          // It could be possible that the destination naming strategy is different from the source
          String subjectName =
              subjectNameStrategy.subjectName(topic, isKey, schemaAndDestId.schema);
          schemaAndDestId.id =
              destSchemaRegistryClient.register(subjectName, schemaAndDestId.schema);
          schemaCache.put(sourceSchemaId, schemaAndDestId);
        } catch (IOException | RestClientException e) {
          log.error(
              String.format(
                  "Unable to register source schema id %d to destination registry for record %s",
                  sourceSchemaId, recordPart),
              e);
          return Optional.empty();
        }
      }
    } else {
      throw new SerializationException("Unknown magic byte!");
    }
    return Optional.ofNullable(schemaAndDestId.id);
  }

  @Override
  public void close() {
    this.sourceSchemaRegistryClient = null;
    this.destSchemaRegistryClient = null;
  }

  interface ConfigName {
    String SRC_SCHEMA_REGISTRY_URL =
        "src." + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    String SRC_BASIC_AUTH_CREDENTIALS_SOURCE =
        "src." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
    String SRC_USER_INFO = "src." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;
    String DEST_SCHEMA_REGISTRY_URL =
        "dest." + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    String DEST_BASIC_AUTH_CREDENTIALS_SOURCE =
        "dest." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
    String DEST_USER_INFO = "dest." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;
    String DEST_AUTO_REGISTER_SCHEMAS =
        "dest." + AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
    String DEST_USE_LATEST_VERSION = "dest." + AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION;
    String DEST_LATEST_COMPATIBILITY_STRICT =
        "dest." + AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT;
    String SCHEMA_CAPACITY = "schema.capacity";
    String TRANSFER_KEYS = "transfer.message.keys";
    String INCLUDE_HEADERS = "include.message.headers";
  }

  private static class SchemaAndId {
    private Integer id;
    private AvroSchema schema;

    SchemaAndId() {}

    SchemaAndId(int id, AvroSchema schema) {
      this.id = id;
      this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaAndId schemaAndId = (SchemaAndId) o;
      return Objects.equals(id, schemaAndId.id) && Objects.equals(schema, schemaAndId.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, schema);
    }

    @Override
    public String toString() {
      return "SchemaAndId{" + "id=" + id + ", schema=" + schema + '}';
    }
  }
}

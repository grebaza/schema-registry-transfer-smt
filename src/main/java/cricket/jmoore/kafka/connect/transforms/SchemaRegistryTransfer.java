/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

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

  private static int smtCalls = 0;
  private static final byte MAGIC_BYTE = (byte) 0x0;
  // wire-format is magic byte + an integer, then data
  private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

  public static final String OVERVIEW_DOC =
      "Inspect the Confluent KafkaSchemaSerializer's wire-format header to copy schemas from one Schema Registry to another.";
  public static final String SCHEMA_CAPACITY_CONFIG_DOC =
      "The maximum amount of schemas to be stored for each Schema Registry client.";
  public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;

  // source registry configuration
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

  // destination registry configuration
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
  public static final String DEST_COMPATIBILITY_TYPE_DOC =
      DEST_PREAMBLE + "Compatibility type set for schemas. No changes if empty.";
  public static final String DEST_COMPATIBILITY_TYPE_DEFAULT = "";

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
  private String schemaCompatibility;
  private String currentSchemaCompatibility;

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
                ConfigName.DEST_COMPATIBILITY_TYPE,
                ConfigDef.Type.STRING,
                DEST_COMPATIBILITY_TYPE_DEFAULT,
                ConfigDef.Importance.LOW,
                DEST_COMPATIBILITY_TYPE_DOC)
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

    // source registry config
    List<String> sourceUrls = config.getList(ConfigName.SRC_SCHEMA_REGISTRY_URL);
    final Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "SRC_" + config.getString(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE));
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
        config.getPassword(ConfigName.SRC_USER_INFO).value());

    // destination registry config
    List<String> destUrls = config.getList(ConfigName.DEST_SCHEMA_REGISTRY_URL);
    final Map<String, String> destProps = new HashMap<>();
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "DEST_" + config.getString(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE));
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
        config.getPassword(ConfigName.DEST_USER_INFO).value());

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
    this.schemaCompatibility = config.getString(ConfigName.DEST_COMPATIBILITY_TYPE);
    log.warn("compatibility.test: {}", schemaCompatibility);
  }

  @Override
  public R apply(R r) {
    final String topic = r.topic();

    // Transcribe the key's schema id
    final Object key = r.key();
    final Schema keySchema = r.keySchema();

    log.trace("Iteration: {}", ++smtCalls);
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

        destSchemaId = copyAvroSchema(b, topic, isKey);
        b.putInt(
            1,
            destSchemaId.orElseThrow(
                () ->
                    new ConnectException(
                        String.format(
                            "Transform failed. Unable to update schema id for record %s",
                            recordPart))));
        updatedKeyValue = (Object) b.array();
      }
    } else {
      throw new ConnectException(
          String.format("Transform failed. Record %s does not have a byte[] schema.", recordPart));
    }

    return updatedKeyValue;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  static <T> Supplier<Optional<T>> OptionalSupplier(ThrowingSupplier<T> supplier) {
    return () -> {
      try {
        return Optional.ofNullable(supplier.get());
      } catch (Exception e) {
        return Optional.empty();
      }
    };
  }

  static <T> Supplier<T> RethrowingSupplier(ThrowingSupplier<T> supplier) {
    return () -> {
      try {
        return supplier.get();
      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private boolean subjectExists(String subjectName, String recordPart) {
    try {
      this.currentSchemaCompatibility = destSchemaRegistryClient.getCompatibility(subjectName);
    } catch (IOException | RestClientException e) {
      log.warn(
          "Unable to get compatibility type of subject {} for record {}", subjectName, recordPart);
      return false;
    }
    return true;
  }

  protected Optional<Integer> copyAvroSchema(ByteBuffer buffer, String topic, boolean isKey) {
    SchemaAndId schemaAndDestId;
    final String recordPart = isKey == true ? "key" : "value";

    if (buffer.get() == MAGIC_BYTE) {
      int sourceSchemaId = buffer.getInt();

      // Lookup schema (first in Cache, and if not found, in source registry)
      if (smtCalls < 100) {
        log.warn("Looking up schema id {} in Cache for record {}", sourceSchemaId, recordPart);
      }
      schemaAndDestId = schemaCache.get(sourceSchemaId);
      if (schemaAndDestId != null) {
        if (smtCalls < 5) {
          log.warn(
              "Schema id {} found at Cache for record {}. Not registering in destination registry",
              sourceSchemaId,
              recordPart);
        }
      } else {
        log.warn("Schema id {} not found at Cache for record {}", sourceSchemaId, recordPart);
        schemaAndDestId = new SchemaAndId();
        try {
          log.warn(
              "Looking up schema id {} in source registry for record {}",
              sourceSchemaId,
              recordPart);
          // can't do getBySubjectAndId because that requires a Schema object for the strategy
          schemaAndDestId.schema =
              (AvroSchema) sourceSchemaRegistryClient.getSchemaById(sourceSchemaId);
        } catch (IOException | RestClientException e) {
          log.error(
              "Unable to fetch schema id {} in source registry for record {}",
              sourceSchemaId,
              recordPart);
          throw new ConnectException(e);
        }

        // Get subject from SubjectNameStrategy
        String subjectName = subjectNameStrategy.subjectName(topic, isKey, schemaAndDestId.schema);

        // Update compatibility type on destination registry (if necessary)
        // Subject exists or not
        /*if (subjectExists(subjectName, recordPart)) {
          if (!schemaCompatibility.equals(currentSchemaCompatibility)) {
            try {
              log.warn("Subject name {}", subjectName);
              String compatibilityLevelUpdated = updateCompatibility(subjectName);
              log.warn("Schema compatibility updated to {}", compatibilityLevelUpdated);
            } catch (IOException | RestClientException e) {
              log.error(
                "Unable to update compatibility type of subject {} for record {}",
                subjectName,
                recordPart);
              throw new ConnectException(e);
            }
          }
        }*/
        // Get schema id on destination registry (registering if necessary)
        try {
          final AvroSchema schema = schemaAndDestId.schema;
          schemaAndDestId.id =
              OptionalSupplier(() -> destSchemaRegistryClient.getId(subjectName, schema))
                  .get()
                  .orElseGet(
                      RethrowingSupplier(
                          () -> {
                            int id = destSchemaRegistryClient.register(subjectName, schema);
                            if (!schemaCompatibility.isEmpty()) {
                              String compatibilityLevelRegistered =
                                  updateCompatibility(subjectName);
                              log.warn(
                                  "Schema compatibility registered as {} for subject {}",
                                  compatibilityLevelRegistered,
                                  subjectName);
                            }
                            return id;
                          }));
        } catch (RuntimeException e) {
          log.error(
              "Unable to get or register schema id {} into destination registry for record {}",
              sourceSchemaId,
              recordPart);
          return Optional.empty();
        }
        // Update Schema Cache
        schemaCache.put(sourceSchemaId, schemaAndDestId);
      }
    } else {
      throw new SerializationException("Unknown magic byte!");
    }
    return Optional.ofNullable(schemaAndDestId.id);
  }

  private String updateCompatibility(String subject) throws IOException, RestClientException {
    try {
      return destSchemaRegistryClient.updateCompatibility(subject, schemaCompatibility);
    } catch (IOException | RestClientException e) {
      log.error("Unable to change compatibility type for subject {}", subject);
      throw e;
    }
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
    String DEST_COMPATIBILITY_TYPE = "dest.compatibility.type";
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

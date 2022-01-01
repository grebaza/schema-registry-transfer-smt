/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

public class SchemaRegistryTransferConfig extends AbstractConfig {

  private static final String OVERVIEW_DOC =
      "Inspect the Confluent KafkaSchemaSerializer's wire-format header to "
          + "copy schemas from one Schema Registry to another.";
  private static final String SRC_PREAMBLE = "For source consumer's schema registry, ";
  private static final String DEST_PREAMBLE = "For target producer's schema registry, ";

  public static final String SRC_SCHEMA_REGISTRY_URL =
      "src." + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SRC_SCHEMA_REGISTRY_DOC =
      "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";

  public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE =
      "src." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
  private static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_DOC =
      SRC_PREAMBLE + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
  public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;

  public static final String SRC_USER_INFO =
      "src." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;
  private static final String SRC_USER_INFO_DOC =
      SRC_PREAMBLE + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
  public static final String SRC_USER_INFO_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

  public static final String DEST_SCHEMA_REGISTRY_URL =
      "dest." + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String DEST_SCHEMA_REGISTRY_DOC =
      "A list of addresses for the Schema Registry to copy to. The producer's Schema Registry.";

  public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE =
      "dest." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
  private static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
  public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;

  public static final String DEST_USER_INFO =
      "dest." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;
  private static final String DEST_USER_INFO_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
  public static final String DEST_USER_INFO_DEFAULT =
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

  public static final String DEST_COMPATIBILITY_TYPE = "dest.compatibility.type";
  private static final String DEST_COMPATIBILITY_TYPE_DOC =
      DEST_PREAMBLE + "Compatibility type set for schemas. No changes if empty.";
  public static final String DEST_COMPATIBILITY_TYPE_DEFAULT = "";

  public static final String SCHEMA_CAPACITY = "schema.capacity";
  private static final String SCHEMA_CAPACITY_DOC =
      "The maximum amount of schemas to be stored for each Schema Registry client.";
  public static final Integer SCHEMA_CAPACITY_DEFAULT = 100;

  public static final String TRANSFER_KEYS = "transfer.message.keys";
  private static final String TRANSFER_KEYS_DOC =
      "Whether or not to copy message key schemas between registries.";
  public static final Boolean TRANSFER_KEYS_DEFAULT = true;

  public static final String INCLUDE_HEADERS = "include.message.headers";
  private static final String INCLUDE_HEADERS_DOC =
      "Whether or not to preserve the Kafka Connect Record headers.";
  public static final Boolean INCLUDE_HEADERS_DEFAULT = true;

  public static final String DEST_KEY_SUBJECT_NAME_STRATEGY =
      AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY;
  private static final String DEST_KEY_SUBJECT_NAME_STRATEGY_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY_DOC;

  public static final String DEST_VALUE_SUBJECT_NAME_STRATEGY =
      AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
  private static final String DEST_VALUE_SUBJECT_NAME_STRATEGY_DOC =
      DEST_PREAMBLE + AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY_DOC;

  // config variables
  public CachedSchemaRegistryClient sourceSchemaRegistryClient;
  public CachedSchemaRegistryClient destSchemaRegistryClient;
  public boolean transferKeys, includeHeaders;

  // caches from the source registry to the destination registry
  public Cache<Integer, ParsedSchemaAndId> schemaCache;
  public String newSchemaCompatibility;

  // Name Strategy for Subject (Key and Value)
  public SubjectNameStrategy keySubjectNameStrategy;
  public SubjectNameStrategy valueSubjectNameStrategy;

  public SchemaRegistryTransferConfig(Map<String, ?> originals) {
    super(config(), originals);

    // source registry config
    List<String> sourceUrls = getList(SRC_SCHEMA_REGISTRY_URL);
    final Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "SRC_" + getString(SRC_BASIC_AUTH_CREDENTIALS_SOURCE));
    sourceProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, getPassword(SRC_USER_INFO).value());

    // destination registry config
    List<String> destUrls = getList(DEST_SCHEMA_REGISTRY_URL);
    final Map<String, String> destProps = new HashMap<>();
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
        "DEST_" + getString(DEST_BASIC_AUTH_CREDENTIALS_SOURCE));
    destProps.put(
        AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, getPassword(DEST_USER_INFO).value());

    Integer schemaCapacity = getInt(SCHEMA_CAPACITY);
    schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
    sourceSchemaRegistryClient =
        new CachedSchemaRegistryClient(sourceUrls, schemaCapacity, sourceProps);
    destSchemaRegistryClient = new CachedSchemaRegistryClient(destUrls, schemaCapacity, destProps);
    transferKeys = getBoolean(TRANSFER_KEYS);
    includeHeaders = getBoolean(INCLUDE_HEADERS);

    newSchemaCompatibility = getString(DEST_COMPATIBILITY_TYPE);

    // Strategy for the subjects
    keySubjectNameStrategy =
        getConfiguredInstance(DEST_KEY_SUBJECT_NAME_STRATEGY, SubjectNameStrategy.class);
    valueSubjectNameStrategy =
        getConfiguredInstance(DEST_VALUE_SUBJECT_NAME_STRATEGY, SubjectNameStrategy.class);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            SRC_SCHEMA_REGISTRY_URL,
            ConfigDef.Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            new NonEmptyListValidator(),
            ConfigDef.Importance.HIGH,
            SRC_SCHEMA_REGISTRY_DOC)
        .define(
            DEST_SCHEMA_REGISTRY_URL,
            ConfigDef.Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            new NonEmptyListValidator(),
            ConfigDef.Importance.HIGH,
            DEST_SCHEMA_REGISTRY_DOC)
        .define(
            SRC_BASIC_AUTH_CREDENTIALS_SOURCE,
            ConfigDef.Type.STRING,
            SRC_BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            SRC_BASIC_AUTH_CREDENTIALS_SOURCE_DOC)
        .define(
            SRC_USER_INFO,
            ConfigDef.Type.PASSWORD,
            SRC_USER_INFO_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            SRC_USER_INFO_DOC)
        .define(
            DEST_BASIC_AUTH_CREDENTIALS_SOURCE,
            ConfigDef.Type.STRING,
            DEST_BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DEST_BASIC_AUTH_CREDENTIALS_SOURCE_DOC)
        .define(
            DEST_USER_INFO,
            ConfigDef.Type.PASSWORD,
            DEST_USER_INFO_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DEST_USER_INFO_DOC)
        .define(
            DEST_COMPATIBILITY_TYPE,
            ConfigDef.Type.STRING,
            DEST_COMPATIBILITY_TYPE_DEFAULT,
            ConfigDef.Importance.LOW,
            DEST_COMPATIBILITY_TYPE_DOC)
        .define(
            SCHEMA_CAPACITY,
            ConfigDef.Type.INT,
            SCHEMA_CAPACITY_DEFAULT,
            ConfigDef.Importance.LOW,
            SCHEMA_CAPACITY_DOC)
        .define(
            TRANSFER_KEYS,
            ConfigDef.Type.BOOLEAN,
            TRANSFER_KEYS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TRANSFER_KEYS_DOC)
        .define(
            INCLUDE_HEADERS,
            ConfigDef.Type.BOOLEAN,
            INCLUDE_HEADERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            INCLUDE_HEADERS_DOC)
        .define(
            DEST_KEY_SUBJECT_NAME_STRATEGY,
            ConfigDef.Type.CLASS,
            TopicNameStrategy.class,
            ConfigDef.Importance.MEDIUM,
            DEST_KEY_SUBJECT_NAME_STRATEGY_DOC)
        .define(
            DEST_VALUE_SUBJECT_NAME_STRATEGY,
            ConfigDef.Type.CLASS,
            TopicNameStrategy.class,
            ConfigDef.Importance.MEDIUM,
            DEST_VALUE_SUBJECT_NAME_STRATEGY_DOC);
    // todo: Other properties might be useful
  }
}

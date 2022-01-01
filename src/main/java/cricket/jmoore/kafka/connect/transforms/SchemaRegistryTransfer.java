/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

@SuppressWarnings("unused")
public class SchemaRegistryTransfer<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryTransfer.class);

  private static final byte MAGIC_BYTE = (byte) 0x0;
  private static final int KEY_VALUE_MIN_LEN = 5;
  // wire-format is magic byte + an integer, then data
  private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

  private SchemaRegistryTransferConfig config;
  private static int smtCalls = 0;

  public SchemaRegistryTransfer() {}

  @Override
  public ConfigDef config() {
    return SchemaRegistryTransferConfig.config();
  }

  @Override
  public void configure(Map<String, ?> props) {
    this.config = new SchemaRegistryTransferConfig(props);
  }

  @Override
  public void close() {
    this.config = null;
  }

  @Override
  public R apply(R r) {
    final String topic = r.topic();

    // Transcribe the key's schema id
    final Object key = r.key();
    final Schema keySchema = r.keySchema();

    log.trace("Iteration: {}", ++smtCalls);
    Object updatedKey = key;
    if (config.transferKeys) {
      updatedKey = updateKeyValue(key, keySchema, topic, true);
    } else {
      log.debug(
          "Skipping record key translation. {} has been to false. Keys will be passed as-is.",
          SchemaRegistryTransferConfig.TRANSFER_KEYS);
    }

    // Transcribe the value's schema id
    final Object value = r.value();
    final Schema valueSchema = r.valueSchema();

    Object updatedValue = updateKeyValue(value, valueSchema, topic, false);

    return config.includeHeaders
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
        log.debug("Passing through null record {}.", recordPart);
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

        destSchemaId = translateRegistrySchema(b, topic, isKey);
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

  private Optional<Integer> translateRegistrySchema(
      ByteBuffer buffer, String topic, boolean isKey) {
    ParsedSchemaAndId schemaAndDestId;
    final String recordPart = isKey == true ? "key" : "value";

    if (buffer.get() == MAGIC_BYTE) {
      int sourceSchemaId = buffer.getInt();

      // Lookup schema (first in Cache, and if not found, in source registry)
      log.debug("Looking up schema id {} in Cache for record {}", sourceSchemaId, recordPart);
      schemaAndDestId = config.schemaCache.get(sourceSchemaId);
      if (schemaAndDestId != null) {
        log.debug(
            "Schema id {} found at Cache for record {}. Not registering in destination registry",
            sourceSchemaId,
            recordPart);
      } else {
        log.debug("Schema id {} not found at Cache for record {}", sourceSchemaId, recordPart);
        schemaAndDestId = new ParsedSchemaAndId();
        try {
          log.debug(
              "Looking up schema id {} in source registry for record {}",
              sourceSchemaId,
              recordPart);
          // can't do getBySubjectAndId because that requires a Schema object for the strategy
          schemaAndDestId.schema = config.sourceSchemaRegistryClient.getSchemaById(sourceSchemaId);
        } catch (IOException | RestClientException e) {
          log.error(
              "Unable to fetch schema id {} in source registry for record {}",
              sourceSchemaId,
              recordPart);
          throw new ConnectException(e);
        }

        // Get subject from SubjectNameStrategy
        String subjectName =
            isKey
                ? config.keySubjectNameStrategy.subjectName(topic, isKey, schemaAndDestId.schema)
                : config.valueSubjectNameStrategy.subjectName(topic, isKey, schemaAndDestId.schema);
        log.debug("Subject on destination registry {}", subjectName);
        String schemaCompatibility = getSchemaCompatibility(subjectName);
        boolean isSubjectOnRegistry = schemaCompatibility == null ? false : true;
        boolean isSchemaCompatibilityForChange =
            !config.newSchemaCompatibility.equals(
                schemaCompatibility == null ? "" : schemaCompatibility);

        // Get schema id on destination registry (registering if necessary)
        log.debug(
            "Registering schema on destination registry for subject {} and schema id {}",
            subjectName,
            sourceSchemaId);
        try {
          final ParsedSchema schema = schemaAndDestId.schema;
          schemaAndDestId.id =
              Utils.OptionalSupplier(
                      () -> config.destSchemaRegistryClient.getId(subjectName, schema))
                  .get()
                  .orElseGet(
                      Utils.RethrowingSupplier(
                          () -> {
                            int id;
                            if (isSubjectOnRegistry) {
                              if (isSchemaCompatibilityForChange)
                                updateSchemaCompatibility(subjectName);
                              id = config.destSchemaRegistryClient.register(subjectName, schema);
                            } else {
                              id = config.destSchemaRegistryClient.register(subjectName, schema);
                              if (isSchemaCompatibilityForChange)
                                updateSchemaCompatibility(subjectName);
                            }
                            return id;
                          }));
        } catch (RuntimeException e) {
          return Optional.empty();
        }
        // Update Schema Cache
        config.schemaCache.put(sourceSchemaId, schemaAndDestId);
      }
    } else {
      throw new SerializationException("Unknown magic byte!");
    }
    return Optional.ofNullable(schemaAndDestId.id);
  }

  private String getSchemaCompatibility(String subject) {
    try {
      log.debug("get schema compatibility type for subject {}", subject);
      return config.destSchemaRegistryClient.getCompatibility(subject);
    } catch (IOException | RestClientException e) {
      log.warn("Unable to get schema compatibility type of subject {}", subject);
      return null;
    }
  }

  private String updateSchemaCompatibility(String subject) throws IOException, RestClientException {
    try {
      log.trace("Updating compatibility type of subject {}...", subject);
      String compatibility =
          config.destSchemaRegistryClient.updateCompatibility(
              subject, config.newSchemaCompatibility);
      log.debug("Schema compatibility registered as {} for subject {}", compatibility, subject);
      return compatibility;
    } catch (IOException | RestClientException e) {
      log.error("Unable to change schema compatibility type for subject {}", subject);
      throw e;
    }
  }
}

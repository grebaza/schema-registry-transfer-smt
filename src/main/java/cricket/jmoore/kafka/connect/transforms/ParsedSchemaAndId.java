/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.util.Objects;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public class ParsedSchemaAndId {
  protected Integer id;
  protected ParsedSchema schema;

  ParsedSchemaAndId() {}

  ParsedSchemaAndId(int id, ParsedSchema schema) {
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
    ParsedSchemaAndId schemaAndId = (ParsedSchemaAndId) o;
    return Objects.equals(id, schemaAndId.id) && Objects.equals(schema, schemaAndId.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, schema);
  }

  @Override
  public String toString() {
    return "ParsedSchemaAndId{" + "id=" + id + ", schema=" + schema + '}';
  }
}

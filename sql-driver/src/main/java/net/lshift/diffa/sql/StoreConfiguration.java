package net.lshift.diffa.sql;

import org.jooq.*;

import static org.jooq.impl.Factory.*;

public class StoreConfiguration {

  private DynamicTable table;

  private String idFieldName;
  private String versionFieldName;
  private String partitionBy;

  public StoreConfiguration(String tableName) {
    this.table = new DynamicTable(tableName.toUpperCase());
  }

  public <T> StoreConfiguration withId(String fieldName, DataType<T> type) {
    idFieldName = fieldName.toUpperCase();
    return withField(idFieldName, type);
  }

  public <T> StoreConfiguration withVersion(String fieldName, DataType<T> type) {
    versionFieldName = fieldName.toUpperCase();
    return withField(versionFieldName, type);
  }

  public <T> StoreConfiguration partitionBy(String fieldName, DataType<T> type) {
    partitionBy = fieldName.toUpperCase();
    return withField(partitionBy, type);
  }

  public Table<Record> getTable() {
    return table;
  }

  public Field<?> partitionBy() {
    return getField(partitionBy);
  }

  public Field<?> getId() {
    return getField(idFieldName);
  }

  public Field<?> getVersion() {
    return getField(versionFieldName);
  }

  private Field<?> getField(String fieldName) {

    if (fieldName == null) {
      throw new RuntimeException("Undefined field: " + fieldName);
    }

    return table.getField(fieldName);
  }

  private <T> StoreConfiguration withField(String fieldName, DataType<T> type) {
    this.table.addField(fieldName, type);
    return this;
  }
}

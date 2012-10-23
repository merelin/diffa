package net.lshift.diffa.versioning;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LoggingBatchMutator implements BatchMutator {

  static Logger log = LoggerFactory.getLogger(LoggingBatchMutator.class);

  private Mutator<String> mutator;

  private List<Mutation> mutations = new ArrayList<Mutation>();

  public LoggingBatchMutator(Keyspace keyspace) {
    mutator = HFactory.createMutator(keyspace, StringSerializer.get());
  }

  public void execute() {
    mutator.execute();
    for (Mutation mutation : mutations) {
      log.info(mutation.toString());
    }
  }

  @Override
  public void insertColumn(String rowKey, String columnFamily, String columnName, String columnValue) {
    addMutation(rowKey, columnFamily, columnName, columnValue);
    mutator.addInsertion(rowKey, columnFamily,  HFactory.createStringColumn(columnName, columnValue));
  }

  @Override
  public void insertDateColumn(String rowKey, String columnFamily, String columnName, DateTime columnValue) {
    // TODO use a serializer for Joda DateTime
    addMutation(rowKey, columnFamily, columnName, columnValue);
    mutator.addInsertion(rowKey, columnFamily,  HFactory.createColumn(columnName, columnValue.toDate(), StringSerializer.get(), DateSerializer.get()));
  }

  @Override
  public void invalidateColumn(String rowKey, String columnFamily, String columnName) {
    addInvalidation(rowKey, columnFamily, columnName);
    mutator.addInsertion(rowKey, columnFamily, HFactory.createStringColumn(columnName,""));
  }

  @Override
  public void insertColumn(String rowKey, String columnFamily, HColumn<String, Boolean> column) {
    addMutation(rowKey, columnFamily, column.getName(), column.getValue());
    mutator.addInsertion(rowKey, columnFamily, column);
  }

  private void addInvalidation(String rowKey, String columnFamily, String columnName) {
    Mutation mutation = new Mutation(rowKey, columnFamily, columnName, true);
    mutations.add(mutation);
  }

  private void addMutation(String rowKey, String columnFamily, String columnName, Object columnValue) {
    Mutation mutation = new Mutation(rowKey, columnFamily, columnName, columnValue);
    mutations.add(mutation);
  }

  private class Mutation {

    String rowKey;
    String columnFamily;
    String columnName;
    Object columnValue;
    boolean isInvalidation = false;

    private Mutation(String rowKey, String columnFamily, String columnName, Object columnValue) {
      this.rowKey = rowKey;
      this.columnFamily = columnFamily;
      this.columnName = columnName;
      this.columnValue = columnValue;
    }

    private Mutation(String rowKey, String columnFamily, String columnName, Object columnValue, boolean isInvalidation) {
      this.rowKey = rowKey;
      this.columnFamily = columnFamily;
      this.columnName = columnName;
      this.columnValue = columnValue;
      this.isInvalidation = isInvalidation;
    }

    @Override
    public String toString() {
      return "Mutation{" +
          "rowKey='" + rowKey + '\'' +
          ", columnFamily='" + columnFamily + '\'' +
          ", columnName='" + columnName + '\'' +
          ", columnValue=" + columnValue +
          ", isInvalidation=" + isInvalidation +
          '}';
    }
  }
}

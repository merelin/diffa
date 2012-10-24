package net.lshift.diffa.versioning;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.util.*;

public class LoggingBatchMutator implements BatchMutator {

  static Logger log = LoggerFactory.getLogger(LoggingBatchMutator.class);

  private Mutator<String> mutator;

  private List<Mutation> mutations = new ArrayList<Mutation>();

  public LoggingBatchMutator(Keyspace keyspace) {
    mutator = HFactory.createMutator(keyspace, StringSerializer.get());
  }

  public void execute() {
    mutator.execute();

    StringBuilder sb = new StringBuilder();
    sb.append("\n\n");

    Function<Mutation, String> columnFamily = new Function<Mutation, String>() {
      public String apply(Mutation mutation) {
        return mutation.columnFamily;
      }
    };

    ImmutableListMultimap<String,Mutation> grouped = Multimaps.index(mutations,columnFamily);
    Iterator<String> it = grouped.keySet().iterator();

    while (it.hasNext()) {
      String cf = it.next();

      sb.append(cf).append("\n");
      sb.append(StringUtils.repeat("-", cf.length()));
      sb.append("\n");

      ImmutableList<Mutation> mutations = grouped.get(cf);
      for (Mutation mutation : mutations) {
        sb.append(mutation.rowKey).append(": ");

        if (mutation.isInvalidation) {
          sb.append(" invalidated [").append(mutation.columnName).append("]");
        } else {
          sb.append(mutation.columnName).append(" [").append(mutation.columnValue).append("]");
        }


        sb.append("\n");
      }
      sb.append("\n");
    }

    log.info(sb.toString());
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

    private Mutation(String rowKey, String columnFamily, String columnName, boolean isInvalidation) {
      this.rowKey = rowKey;
      this.columnFamily = columnFamily;
      this.columnName = columnName;
      this.isInvalidation = isInvalidation;
    }

  }
}

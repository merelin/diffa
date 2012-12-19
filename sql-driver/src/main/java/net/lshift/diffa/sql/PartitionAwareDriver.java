package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.scanning.ScanResultHandler;
import net.lshift.diffa.scanning.Scannable;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.jooq.impl.Factory.*;

public class PartitionAwareDriver extends AbstractDatabaseAware implements Scannable {

  static Logger log = LoggerFactory.getLogger(PartitionAwareDriver.class);

  private PartitionMetadata config;

  private static HashMap<SQLDialect, String> md5FunctionDefinitions = new HashMap<SQLDialect, String>();

  static {
    md5FunctionDefinitions.put(SQLDialect.HSQLDB,
        "create function md5(v varchar(32672)) returns varchar(32) language java deterministic no sql external name 'CLASSPATH:org.apache.commons.codec.digest.DigestUtils.md5Hex'");
    md5FunctionDefinitions.put(SQLDialect.ORACLE,
        "CREATE OR REPLACE FUNCTION md5 (input_string VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN dbms_obfuscation_toolkit.md5(input_string => inputString); END;");
  }

  @Inject
  public PartitionAwareDriver(DataSource ds, PartitionMetadata config) {
    super(ds);
    this.config = config;

    Connection connection = getConnection();
    Factory db = getFactory(connection);

    String functionDefinition = md5DefinitionForDialect(db.getDialect());

    try {

      // TODO Hack - Just try to create the function, if it fails, we just assume that it already exists
      // It is probably a better idea to introspect the information schema to find out for sure that this function is available

      db.execute(functionDefinition);
      log.info("Created md5 function");
    }
    catch (Exception e) {

      if (e.getMessage().contains("already exists")) {
        // Crude way of ignoring
        log.info("Did not create md5 function, since it looks like it already exists in the target database");
      }
      else {
        log.info("Find out whether we need to care about this", e);
      }

    }

  }
  private String md5DefinitionForDialect(SQLDialect dialect) {
    return md5FunctionDefinitions.get(dialect);
  }

  @Override
  public void scan(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize, ScanResultHandler handler) {

    Connection connection = getConnection();
    Factory db = getFactory(connection);

    Table<Record> underlyingTable = config.getTable();

    Table<Record> A = underlyingTable.as("a");
    Table<Record> B = underlyingTable.as("b");

    /**
     * This is pretty dodgy - we assume that anything you can md5 must be coercible to a VARCHAR for the purposes
     * of this query - this is probably not generally the case for any type of query, but until it blows
     * up for us, we assume it to hold true.
     */
    Field<String> underlyingId = (Field<String>) config.getId();
    Field<String> underlyingVersion = (Field<String>) config.getVersion();

    /**
     * OTOH we do need to do something more sensible with partition because we need to slice and dice in different
     * ways for different data types.
     */
    Field<?> underlyingPartition = config.partitionBy();

    Field<String> A_ID = A.getField(underlyingId);
    Field<String> B_ID = B.getField(underlyingId);
    Field<String> A_VERSION = A.getField(underlyingVersion);

    DataType<?> partitionType = underlyingPartition.getDataType();

    if (partitionType.equals(SQLDataType.DATE) || partitionType.equals(SQLDataType.TIMESTAMP)) {
      Field<Date> typedField = (Field<Date>) underlyingPartition;
      underlyingPartition = A.getField(typedField);
    }
    else {
      throw new RuntimeException("Currently we can not handle partition type: " + underlyingPartition.getDataType() + " ; please contact your nearest software developer");
    }

    Field<Object> day = field("DAY");
    Field<Object> month = field("MONTH");
    Field<Object> year = field("YEAR");

    Field<Object> bucket = field("BUCKET");
    Field<Object> version = field("VERSION");
    Field<Object> id = field("ID");
    Field<Object> digest = field("DIGEST");

    Field<Date> truncDay = Factory.field("trunc({0}, {1})", SQLDataType.DATE, underlyingPartition, inline("DD"));
    Field<Date> truncMonth = Factory.field("trunc({0}, {1})", SQLDataType.DATE, day, inline("MM"));
    Field<Date> truncYear = Factory.field("trunc({0}, {1})", SQLDataType.DATE, month, inline("YY"));

    Field<Integer> bucketCount = Factory.cast(ceil(cast(count(), SQLDataType.REAL).div(maxSliceSize)), SQLDataType.INTEGER);

    SelectHavingStep slicedBuckets =
        db.select(day, bucket, function("md5", String.class, groupConcat(version).orderBy(id.asc()).separator("")).as(digest.getName())).
            from(
                db.select(truncDay.as(day.getName()), A_ID.as(id.getName()), A_VERSION.as(version.getName()), bucketCount.as(bucket.getName())).
                    from(A).
                    join(B).
                    on(A_ID.eq(B_ID)).
                    and(A_ID.ge(B_ID)).
                    groupBy(truncDay, A_ID, A_VERSION).
                    orderBy(truncDay, bucket)
            ).
            groupBy(day, bucket);

    SelectLimitStep dailyBuckets =
        db.select(day, function("md5", String.class, groupConcat(digest).orderBy(bucket.asc()).separator("")).as(digest.getName())).
            from(slicedBuckets).
            groupBy(day).
            orderBy(day);

    SelectLimitStep monthlyBuckets =
        db.select(truncMonth.as(month.getName()), function("md5", String.class, groupConcat(digest).orderBy(day.asc()).separator("")).as(digest.getName())).
            from(dailyBuckets).
            groupBy(truncMonth).
            orderBy(month);

    SelectLimitStep yearlyBuckets =
        db.select(truncYear.as(year.getName()), function("md5", String.class, groupConcat(digest).orderBy(month.asc()).separator("")).as(digest.getName())).
            from(monthlyBuckets).
            groupBy(truncYear).
            orderBy(year);

    Cursor<Record> cursor = yearlyBuckets.fetchLazy();

    while(cursor.hasNext()) {
      Record record = cursor.fetchOne();

      Date sqlDate = record.getValueAsDate(year);
      DateTime date = new DateTime(sqlDate.getTime());
      String dateComponent = date.getYear() + "";

      // TODO This attribute is horribly hard coded

      Map<String,String> partition = ImmutableMap.of("bizDate", dateComponent);
      String digestValue = record.getValueAsString(digest);
      ScanResultEntry entry = ScanResultEntry.forAggregate(digestValue, partition);
      handler.onEntry(entry);

    }

    handler.onCompletion();


    closeConnection(connection);
  }

  // TODO This stuff shouldn't really get invoked inline, we should have some kind of wrapping function ....

}

package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableMap;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.jooq.impl.Factory.*;

public class PartitionAwareStore implements PartitionAwareDriver {

  private BoneCPDataSource ds;

  private StoreConfiguration config;

  public PartitionAwareStore(BoneCPDataSource ds, String name, StoreConfiguration config) {

    this.config = config;
    this.ds = ds;

  }

  @Override
  public void scanStore(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize, ScanResultHandler handler) {

    Connection connection = getConnection();
    Factory db = getFactory(connection);

    Table<Record> underlyingTable = config.getTable();

    Table<Record> A = underlyingTable.as("a");
    Table<Record> B = underlyingTable.as("b");

    Field<?> underlyingId = config.getId();
    Field<?> underlyingVersion = config.getVersion();
    Field<?> underlyingPartition = config.partitionBy();

    Condition firstJoinCondition = null;
    Condition secondJoinCondition = null;

    Field<?> A_ID = null;
    Field<?> A_VERSION = null;

    if (underlyingId.getDataType().equals(SQLDataType.VARCHAR)) {
      Field<String> typedField = (Field<String>) underlyingId;
      firstJoinCondition = A.getField(typedField).eq(B.getField(typedField));
      secondJoinCondition = A.getField(typedField).ge(B.getField(typedField));
      A_ID = A.getField(typedField);
    }

    if (underlyingVersion.getDataType().equals(SQLDataType.VARCHAR)) {
      Field<Date> typedField = (Field<Date>) underlyingVersion;
      underlyingVersion = A.getField(typedField);
    }

    if (underlyingPartition.getDataType().equals(SQLDataType.DATE)) {
      Field<Date> typedField = (Field<Date>) underlyingPartition;
      underlyingPartition = A.getField(typedField);
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
                db.select(truncDay.as(day.getName()), A_ID.as(id.getName()), underlyingVersion.as(version.getName()), bucketCount.as(bucket.getName())).
                    from(A).
                    join(B).
                    on(firstJoinCondition).
                    and(secondJoinCondition).
                    groupBy(truncDay, A_ID, A_VERSION).
                    orderBy(truncDay, bucket)
            ).
            groupBy(day, bucket);

    SelectHavingStep dailyBuckets =
        db.select(day, function("md5", String.class, groupConcat(digest).orderBy(bucket.asc()).separator("")).as(digest.getName())).
            from(slicedBuckets).
            groupBy(day);

    SelectHavingStep monthlyBuckets =
        db.select(truncMonth.as(month.getName()), function("md5", String.class, groupConcat(digest).orderBy(day.asc()).separator("")).as(digest.getName())).
            from(dailyBuckets).
            groupBy(truncMonth);

    SelectHavingStep yearlyBuckets =
        db.select(truncYear.as(year.getName()), function("md5", String.class, groupConcat(digest).orderBy(month.asc()).separator("")).as(digest.getName())).
            from(monthlyBuckets).
            groupBy(truncYear);

    Cursor<Record> cursor = yearlyBuckets.fetchLazy();

    while(cursor.hasNext()) {
      Record record = cursor.fetchOne();

      Date sqlDate = record.getValueAsDate(year);
      DateTime date = new DateTime(sqlDate.getTime());
      String dateComponent = date.getYear() + "";

      Map<String,String> partition = ImmutableMap.of("bizDate", dateComponent);
      String digestValue = record.getValueAsString(digest);
      ScanResultEntry entry = ScanResultEntry.forAggregate(digestValue, partition);
      handler.onEntry(entry);

    }


    closeConnection(connection);
  }

  protected Factory getFactory(Connection c) {
    return new Factory(c, SQLDialect.HSQLDB);
  }

  // TODO This stuff shouldn't really get invoked inline, we should have some kind of wrapping function ....

  protected void closeConnection(Connection connection) {
    closeConnection(connection, false);
  }

  protected void closeConnection(Connection connection, boolean shouldCommit) {
    try {
      if (shouldCommit) {
        connection.commit();
      }
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected Connection getConnection() {
    Connection c;

    try {
      c = ds.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return c;
  }
}

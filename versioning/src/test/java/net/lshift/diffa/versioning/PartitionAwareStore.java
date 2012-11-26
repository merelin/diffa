package net.lshift.diffa.versioning;

import com.google.common.collect.ImmutableMap;
import com.googlecode.flyway.core.Flyway;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import static net.lshift.diffa.versioning.tables.Things.THINGS;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;
import net.lshift.diffa.versioning.tables.Things;
import net.lshift.diffa.versioning.tables.records.ThingsRecord;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.jooq.*;
import static org.jooq.impl.Factory.*;
import static org.jooq.impl.Factory.field;

import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.*;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.*;

public class PartitionAwareStore {

  private BoneCPDataSource ds;

  public PartitionAwareStore(String name) {

    try {

      Class.forName("org.hsqldb.jdbcDriver");

      BoneCPConfig config = new BoneCPConfig();
      config.setJdbcUrl("jdbc:hsqldb:mem:" + name);
      config.setUsername("sa");
      config.setPassword("");

      ds = new BoneCPDataSource(config);

      Flyway flyway = new Flyway();
      flyway.setDataSource(ds);
      flyway.migrate();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Factory getFactory(Connection c) {
    return new Factory(c, SQLDialect.HSQLDB);
  }

  public PartitionableThing createRandomThing(Map<String, ?> attributes) {

    PartitionableThing record = new PartitionableThing(attributes);
    record.setId(RandomStringUtils.randomAlphabetic(10));
    record.setVersion(RandomStringUtils.randomAlphabetic(10));

    Connection connection = getConnection();

    Factory db = getFactory(connection);
    db.executeInsert((ThingsRecord)record);

    closeConnection(connection, true);

    return record;
  }

  private void closeConnection(Connection connection) {
    closeConnection(connection, false);
  }

  private void closeConnection(Connection connection, boolean shouldCommit) {
    try {
      if (shouldCommit) {
        connection.commit();
      }
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Connection getConnection() {
    Connection c;

    try {
      c = ds.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return c;
  }

  public Set<ScanResultEntry> scanStore(Set<ScanConstraint> cons, Set<ScanAggregation> aggs, int maxSliceSize) {

    Set<ScanResultEntry> entries = new HashSet<ScanResultEntry>();

    Connection connection = getConnection();
    Factory db = getFactory(connection);

    Things a = THINGS.as("a");
    Things b = THINGS.as("b");

    Field<Object> day = field("DAY");
    Field<Object> month = field("MONTH");
    Field<Object> year = field("YEAR");

    Field<Object> bucket = field("BUCKET");
    Field<Object> version = field("VERSION");
    Field<Object> id = field("ID");
    Field<Object> digest = field("DIGEST");

    Field<Date> truncDay = Factory.field("trunc({0}, {1})", SQLDataType.DATE, a.ENTRY_DATE, inline("DD"));
    Field<Date> truncMonth = Factory.field("trunc({0}, {1})", SQLDataType.DATE, day, inline("MM"));
    Field<Date> truncYear = Factory.field("trunc({0}, {1})", SQLDataType.DATE, month, inline("YY"));

    Field<Integer> bucketCount = Factory.cast(ceil(cast(count(), SQLDataType.REAL).div(maxSliceSize)), SQLDataType.INTEGER);

    SelectHavingStep slicedBuckets =
      db.select(day, bucket, function("md5", String.class, groupConcat(version).orderBy(id.asc()).separator("")).as(digest.getName())).
          from(
              db.select(truncDay.as(day.getName()), a.ID.as(id.getName()), a.VERSION.as(version.getName()), bucketCount.as(bucket.getName())).
                  from(a).
                  join(b).
                    on(a.ID.eq(b.ID)).
                    and(a.ID.ge(b.ID)).
                  groupBy(truncDay, a.ID, a.VERSION).
                  orderBy(truncDay,bucket)
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

    Iterator<Record> it = yearlyBuckets.fetch().iterator();

    while(it.hasNext()) {
      Record record = it.next();

      Date sqlDate = record.getValueAsDate(year);
      DateTime date = new DateTime(sqlDate.getTime());
      String dateComponent = date.getYear() + "";

      Map<String,String> partition = ImmutableMap.of("bizDate", dateComponent);
      String digestValue = record.getValueAsString(digest);
      ScanResultEntry entry = ScanResultEntry.forAggregate(digestValue, partition);
      entries.add(entry);

    }


    closeConnection(connection);

    return entries;
  }


}

package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.lshift.diffa.adapter.scanning.*;
import net.lshift.diffa.scanning.plumbing.BufferingScanResultHandler;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Date;
import java.util.LinkedHashSet;
import java.util.Set;

public class SQLDriverIT extends AbstractDatabaseAware {
  public SQLDriverIT() {
    super(TestDBProvider.createSchema().getDataSource(), TestDBProvider.getDialect());
  }

  @Test
  public void shouldPartitionByDate() throws Exception {

    DateTime start = new DateTime(2005,7,29,0,0,0,0);
    int days = 1000;

    Connection connection = getConnection();

    Factory db = getFactory(connection);

    for (int i = 0; i < days; i++) {
      String id = i + "";
      String version = DigestUtils.md5Hex(id);
      DateTime entryDate = start.plusDays(i);
      Date date = new Date(entryDate.getMillis());
      db.execute("insert into things (id, version,entry_date) values (?,?,?)", id, version, date);
    }

    connection.commit();
    closeConnection(connection, true);

    PartitionMetadata metadata = new PartitionMetadata("THINGS");
    metadata.withId("ID", SQLDataType.VARCHAR).
             withVersion("VERSION", SQLDataType.VARCHAR).
             partitionBy("ENTRY_DATE", SQLDataType.DATE);

    PartitionAwareDriver driver = new PartitionAwareDriver(ds, metadata, TestDBProvider.getDialect());

    ScanAggregation dateAggregation = new DateAggregation("some_date", DateGranularityEnum.Yearly);

    Set<ScanConstraint> cons = null;
    Set<ScanAggregation> aggs = ImmutableSet.of(dateAggregation);

    BufferingScanResultHandler handler = new BufferingScanResultHandler();
    driver.scan(cons, aggs, 100, handler);

    /**
     * Please note that I got this working and then reverse-engineered the results from that code,
     * rather than forwards engineering the expected from some kind of logic ......
     */

    Set<ScanResultEntry> expectedResults = new LinkedHashSet<ScanResultEntry>();
    expectedResults.add(ScanResultEntry.forAggregate("696a51b5982b8521625d39631c1175bb", ImmutableMap.of("bizDate", "2005")));
    expectedResults.add(ScanResultEntry.forAggregate("6b4ee3a8dcf9e71af301b5722406e52f", ImmutableMap.of("bizDate", "2006")));
    expectedResults.add(ScanResultEntry.forAggregate("c7b7eb798fcf835ace16f469c6919e1c", ImmutableMap.of("bizDate", "2007")));
    expectedResults.add(ScanResultEntry.forAggregate("0906f8b73c3e2ff365ff235b3cb020b7", ImmutableMap.of("bizDate", "2008")));

    assertEquals(expectedResults, handler.getEntries());

  }
}

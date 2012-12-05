package net.lshift.diffa.sql;

import com.google.common.collect.ImmutableSet;
import net.lshift.diffa.adapter.scanning.DateAggregation;
import net.lshift.diffa.adapter.scanning.DateGranularityEnum;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.scanning.plumbing.BufferingScanResultHandler;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.jooq.impl.Factory;
import org.jooq.impl.SQLDataType;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Date;
import java.util.Set;

public class SQLDriverIT extends AbstractDatabaseAware {

  public SQLDriverIT() {
    super(TestDBProvider.getHSQLDBDataSource());
  }

  @Test
  public void shouldPartitionByDate() throws Exception {

    String id = RandomStringUtils.randomAlphabetic(10);
    String version = RandomStringUtils.randomAlphabetic(10);
    DateTime date = new DateTime();
    Date sqlDate = new Date(date.getMillis());


    Connection connection = getConnection();

    Factory db = getFactory(connection);
    db.execute("insert into things (id, version,entry_date) values (?,?,?)", id, version, sqlDate);
    connection.commit();
    closeConnection(connection, true);

    StoreConfiguration conf = new StoreConfiguration("THINGS");
    conf.withId("ID", SQLDataType.VARCHAR).
         withVersion("VERSION", SQLDataType.VARCHAR).
         partitionBy("ENTRY_DATE", SQLDataType.DATE);

    PartitionAwareDriver driver = new PartitionAwareDriver(ds, conf);

    ScanAggregation dateAggregation = new DateAggregation("some_date", DateGranularityEnum.Yearly);

    Set<ScanConstraint> cons = null;
    Set<ScanAggregation> aggs = ImmutableSet.of(dateAggregation);

    BufferingScanResultHandler handler = new BufferingScanResultHandler();
    driver.scan(cons, aggs, 100, handler);

    assertFalse(handler.getEntries().isEmpty());

  }
}

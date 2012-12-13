package net.lshift.diffa.conductor;

import com.google.common.collect.ImmutableMap;
import com.googlecode.flyway.core.Flyway;
import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.railyard.RailYard;
import net.lshift.diffa.railyard.RailYardClient;
import net.lshift.diffa.sql.PartitionMetadata;
import net.lshift.diffa.versioning.events.PartitionedEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static net.lshift.diffa.conductor.plumbing.ConfigurationBuilder.buildDataSource;
import static net.lshift.diffa.conductor.plumbing.ConfigurationBuilder.buildMetaData;

public class ConductorIT {

  static String url = "http://localhost:" + ConductorDaemon.DEFAULT_PORT;

  @Test
  public void interviewShouldFinishWhenVersionStoreIsInSyncWithRemoteSystem() throws Exception {

    String space = RandomStringUtils.randomAlphanumeric(10);
    String endpoint = RandomStringUtils.randomAlphanumeric(10);

    DriverConfiguration conf = new DriverConfiguration();
    conf.setTableName("things");
    conf.setIdFieldName("id");
    conf.setVersionFieldName("version");
    conf.setPartitionFieldName("entry_date");
    conf.setDriverClass("org.hsqldb.jdbcDriver");
    conf.setUrl("jdbc:hsqldb:mem:" + RandomStringUtils.randomAlphabetic(5));
    conf.setUsername("sa");
    conf.setPassword("");

    Conductor conductor = new ConductorClient(url);

    conductor.registerDriver(space, endpoint, conf);


    int maxSliceSize = 100;

    DataSource ds = buildDataSource(conf);

    try {

      Flyway flyway = new Flyway();
      flyway.setDataSource(ds);
      flyway.migrate();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PartitionMetadata metadata = buildMetaData(conf);
    PartitionedStore partitionAwareStore = new PartitionedStore(ds, metadata);

    String attributeName = "bizDate";

    // Populate the remote system with some random data

    RailYard railYard = new RailYardClient("http://localhost:7655");


    Random random = new Random();
    DateTime median = new DateTime();
    int itemsInSync = 100;
    List<ChangeEvent> events = new ArrayList<ChangeEvent>();

    for (int i = 0; i < itemsInSync; i++) {
      DateTime randomDate = median.minusDays(random.nextInt(365 * 10));
      PartitionedEvent event = partitionAwareStore.createRandomThing(ImmutableMap.of(attributeName, randomDate));
      events.add(event);

    }

    railYard.postChanges(space, endpoint, events);

    // Begin the interview process

    conductor.begin(space, endpoint);

    /*

    ScanAggregation dateAggregation = new DateAggregation(attributeName, DateGranularityEnum.Yearly);

    Set<ScanConstraint> cons = null;
    Set<ScanAggregation> aggs = ImmutableSet.of(dateAggregation);

    BufferingScanResultHandler handler = new BufferingScanResultHandler();

    partitionAwareStore.scan(cons, aggs, maxSliceSize, handler);

    List<ScanRequest> requests = store.continueInterview(left, cons, aggs, handler.getEntries());

    assertTrue(requests.isEmpty());

    */
  }
}

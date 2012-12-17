package net.lshift.diffa.versioning;

import com.google.common.collect.*;
import com.googlecode.flyway.core.Flyway;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.adapter.scanning.*;
import net.lshift.diffa.events.TombstoneEvent;
import net.lshift.diffa.scanning.*;
import net.lshift.diffa.scanning.http.HttpDriver;
import net.lshift.diffa.scanning.plumbing.BufferingScanResultHandler;
import net.lshift.diffa.sql.PartitionMetadata;
import net.lshift.diffa.versioning.events.PartitionedEvent;
import net.lshift.diffa.versioning.partitioning.AbstractPartitionedEvent;
import net.lshift.diffa.versioning.partitioning.MerkleNode;
import net.lshift.diffa.versioning.partitioning.MerkleUtils;
import net.lshift.diffa.versioning.plumbing.EntityIdBucketing;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jooq.impl.SQLDataType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

public class CassandraVersionStoreIT {

  static Logger log = LoggerFactory.getLogger(CassandraVersionStoreIT.class);


  @Test
  public void u() throws Exception {

    ByteArrayOutputStream os = new ByteArrayOutputStream();


    DatumWriter<net.lshift.diffa.adapter.avro.ScanResultEntry> writer = new SpecificDatumWriter<net.lshift.diffa.adapter.avro.ScanResultEntry>(net.lshift.diffa.adapter.avro.ScanResultEntry.class);

    //DataFileWriter <net.lshift.diffa.scanning.ScanResultEntry> dataFileWriter = new DataFileWriter <net.lshift.diffa.scanning.ScanResultEntry>(writer);
    //dataFileWriter.create(net.lshift.diffa.scanning.ScanResultEntry.SCHEMA$, os);

    writer.setSchema(net.lshift.diffa.adapter.avro.ScanResultEntry.SCHEMA$);

    net.lshift.diffa.adapter.avro.ScanResultEntry e = new net.lshift.diffa.adapter.avro.ScanResultEntry();
    //e.setId("foo");
    e.setVersion("bar");
    //e.setLastUpdated(System.currentTimeMillis());
    //e.setAttributes(new HashMap<CharSequence, CharSequence>());

    Encoder en = EncoderFactory.get().binaryEncoder(os, null);
    writer.write(e, en);
    en.flush();
    writer.write(e, en);
    en.flush();

    //dataFileWriter.append(e);
    //dataFileWriter.flush();



    final DatumReader<net.lshift.diffa.adapter.avro.ScanResultEntry> reader = new SpecificDatumReader<net.lshift.diffa.adapter.avro.ScanResultEntry>(net.lshift.diffa.adapter.avro.ScanResultEntry.class);

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    Decoder d = DecoderFactory.get().binaryDecoder(is, null);

    net.lshift.diffa.adapter.avro.ScanResultEntry e4 = reader.read(null, d);
    net.lshift.diffa.adapter.avro.ScanResultEntry e5 = reader.read(null, d);

    System.err.println(e4);
    System.err.println(e5);

    //DataFileStream<net.lshift.diffa.scanning.ScanResultEntry> dfs = new DataFileStream<net.lshift.diffa.scanning.ScanResultEntry>(is, reader);
    /*
    while (dfs.hasNext()) {
      System.err.println(dfs.next());
    }
    */
    //Decoder decoder = DecoderFactory.get().binaryDecoder(bodyPart.getBodyPartBytes(), null);


  }


  @Test
  public void differingEndpointsShouldProduceDelta() throws Exception {

    CassandraVersionStore store = new CassandraVersionStore();

    final Long left = System.currentTimeMillis() * 2;
    final Long right = left + 1;

    Random random = new Random();

    List<TestablePartitionedEvent> leftEvents = new LinkedList<TestablePartitionedEvent>();
    List<TestablePartitionedEvent> rightEvents = new LinkedList<TestablePartitionedEvent>();

    int itemsInSync = 2;

    for (int i = 0; i < itemsInSync; i++) {

      String id = RandomStringUtils.randomAlphanumeric(10);
      String version = RandomStringUtils.randomAlphanumeric(10);

      TestablePartitionedEvent leftEvent = new DatePartitionedEvent(id, version, random, "transactionDate" );
      TestablePartitionedEvent rightEvent = new StringPartitionedEvent(id, version, "userId");

      insertAtRandomPoint(random, leftEvents, leftEvent);
      insertAtRandomPoint(random, rightEvents, rightEvent);
    }

    Thread leftEventStream = new Thread(new EventStream(store, left, leftEvents));
    Thread rightEventStream = new Thread(new EventStream(store, right, rightEvents));

    leftEventStream.start();
    rightEventStream.start();

    leftEventStream.join();
    rightEventStream.join();

    PairProjection view = new PairProjection(left,  right);

    store.deltify(view);
    TreeLevelRollup firstRollup = store.getDeltaDigest(view);

    assertNotNull(firstRollup);
    assertTrue(firstRollup.isEmpty());

    // mutate an event to produce a delta

    final TestablePartitionedEvent randomLeftEvent = leftEvents.get(random.nextInt(itemsInSync));
    randomLeftEvent.setVersion(RandomStringUtils.randomAlphanumeric(10));

    store.onEvent(left, randomLeftEvent);

    store.deltify(view);
    TreeLevelRollup secondRollup = store.getDeltaDigest(view);

    assertNotNull(secondRollup);
    assertFalse(secondRollup.isEmpty());

    // At this point we could/should walk the delta tree, but we already know which event caused the delta
    // and hence we know the bucket that it belongs to

    MerkleNode leftEntityNode = MerkleUtils.buildEntityIdNode(randomLeftEvent.getId(), randomLeftEvent.getVersion());
    String path = leftEntityNode.getDescendencyPath();

    List<EntityDifference> diffs = store.getOutrightDifferences(view, path);

    assertEquals(1, diffs.size());

    EntityDifference diff = diffs.get(0);

    assertEquals(randomLeftEvent.getId(), diff.getId());
    assertEquals(randomLeftEvent.getVersion(), diff.getLeft());

    // TODO This could be more comprehensive, i.e. it asserts nothing about the RHS of the diff
    /*
    String agent = "http://localhost:19093/diffa-agent/store/scan";
    Scannable scannable = new HttpDriver(agent, "guest", "guest");


    Set<ScanConstraint> cons = new HashSet<ScanConstraint>();

    Set<ScanAggregation> aggs = new HashSet<ScanAggregation>();
    ScanAggregation equivalentAggregation = EntityIdBucketing.getEquivalentAggregation(secondRollup.getBucket());
    aggs.add(equivalentAggregation);

    int maxSliceSize = secondRollup.getMaxSliceSize();

    BufferingScanResultHandler handler = new BufferingScanResultHandler();

    scannable.scan(cons, aggs, maxSliceSize, handler);
    */


  }

  @Test
  public void interviewShouldFinishWhenVersionStoreIsInSyncWithRemoteSystem() throws Exception {

    int maxSliceSize = 100;
    final Long left = System.currentTimeMillis() * 2;

    CassandraVersionStore store = new CassandraVersionStore();
    store.setMaxSliceSize(left, maxSliceSize);

    BoneCPDataSource ds = null;

    try {

      Class.forName("org.hsqldb.jdbcDriver");

      BoneCPConfig config = new BoneCPConfig();
      config.setJdbcUrl("jdbc:hsqldb:mem:" + RandomStringUtils.randomAlphabetic(5));
      config.setUsername("sa");
      config.setPassword("");

      ds = new BoneCPDataSource(config);

      Flyway flyway = new Flyway();
      flyway.setDataSource(ds);
      flyway.migrate();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PartitionMetadata conf = new PartitionMetadata("THINGS");
    conf.withId("ID", SQLDataType.VARCHAR).
         withVersion("VERSION", SQLDataType.VARCHAR).
         partitionBy("ENTRY_DATE", SQLDataType.DATE);

    PartitionAwareThings partitionAwareStore = new PartitionAwareThings(ds, conf);

    String attributeName = "bizDate";

    // Populate the remote system with some random data

    Random random = new Random();
    DateTime median = new DateTime();
    int itemsInSync = 100;

    for (int i = 0; i < itemsInSync; i++) {
      DateTime randomDate = median.minusDays(random.nextInt(365 * 10));
      PartitionedEvent event = partitionAwareStore.createRandomThing(ImmutableMap.of(attributeName, randomDate));
      store.onEvent(left, event);
    }

    // Begin the interview process

    ScanAggregation dateAggregation = new DateAggregation(attributeName, DateGranularityEnum.Yearly);

    Set<ScanConstraint> cons = null;
    Set<ScanAggregation> aggs = ImmutableSet.of(dateAggregation);

    BufferingScanResultHandler handler = new BufferingScanResultHandler();

    partitionAwareStore.scan(cons, aggs, maxSliceSize, handler);

    List<ScanRequest> requests = store.continueInterview(left, cons, aggs, handler.getEntries());

    assertTrue(requests.isEmpty());

  }

  @Test
  public void shouldBeAbleToRoundTripChangeEvent() throws Exception {

    CassandraVersionStore store = new CassandraVersionStore();

    Random random = new Random();

    List<TestablePartitionedEvent> upstreamEvents = new LinkedList<TestablePartitionedEvent>();
    List<TestablePartitionedEvent> downstreamEvents = new LinkedList<TestablePartitionedEvent>();

    int itemsInSync = 20;

    for (int i = 0; i < itemsInSync; i++) {

      String id = RandomStringUtils.randomAlphanumeric(10);
      String version = RandomStringUtils.randomAlphanumeric(10);

      TestablePartitionedEvent upstreamEvent = new DatePartitionedEvent(id, version, random, "transactionDate" );
      TestablePartitionedEvent downstreamEvent = new StringPartitionedEvent(id, version, "userId");

      insertAtRandomPoint(random, upstreamEvents, upstreamEvent);
      insertAtRandomPoint(random, downstreamEvents, downstreamEvent);
    }

    final Long upstream = System.currentTimeMillis() * 2;
    final Long downstream = upstream + 1;

    Thread upstreamEventStream = new Thread(new EventStream(store, upstream, upstreamEvents));
    Thread downstreamEventStream = new Thread(new EventStream(store, downstream, downstreamEvents));

    upstreamEventStream.start();
    downstreamEventStream.start();

    upstreamEventStream.join();
    downstreamEventStream.join();
    /*
    log.info("Initial (uncached) tree query");

    SortedMap<String,BucketDigest> upstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,BucketDigest> downstreamDigests = store.getEntityIdDigests(downstream);

    sanityCheckDigests("", upstreamDigests, downstreamDigests);

    final String firstTopLevelUpstreamDigest = upstreamDigests.get("").getDigest();
    final String firstTopLevelDownstreamDigest = downstreamDigests.get("").getDigest();

    assertEquals(firstTopLevelUpstreamDigest, firstTopLevelDownstreamDigest);

    log.info("Subsequent (cached) tree query");

    store.getEntityIdDigests(upstream);
    store.getEntityIdDigests(downstream);
    */
    final TestablePartitionedEvent randomUpstreamEvent = upstreamEvents.get(random.nextInt(itemsInSync));
    randomUpstreamEvent.setVersion(RandomStringUtils.randomAlphanumeric(10));

    store.onEvent(upstream, randomUpstreamEvent);
    /*
    log.info("Tree query after upstream mutation only (dirty cache)");

    SortedMap<String,BucketDigest> secondUpstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,BucketDigest> secondDownstreamDigests = store.getEntityIdDigests(downstream);

    sanityCheckDigests("", secondUpstreamDigests, secondDownstreamDigests);

    final String secondTopLevelUpstreamDigest = secondUpstreamDigests.get("").getDigest();
    final String secondTopLevelDownstreamDigest = secondDownstreamDigests.get("").getDigest();

    assertEquals(firstTopLevelDownstreamDigest, secondTopLevelDownstreamDigest);
    assertFalse(
      "1st and 2nd upstream digests should be different but were both " + firstTopLevelUpstreamDigest,
      firstTopLevelUpstreamDigest.equals(secondTopLevelUpstreamDigest)
    );
    */
    final String idToDelete = randomUpstreamEvent.getId();

    TombstoneEvent tombstone = new TombstoneEvent() {
      @Override
      public String getId() {
        return idToDelete;
      }
    };

    store.onEvent(upstream, tombstone);
    store.onEvent(downstream, tombstone);

    /*
    log.info("Tree query after upstream and downstream deletions (dirty cache)");

    SortedMap<String,BucketDigest> thirdUpstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,BucketDigest> thirdDownstreamDigests = store.getEntityIdDigests(downstream);

    sanityCheckDigests("", thirdUpstreamDigests, thirdDownstreamDigests);

    final String thirdTopLevelUpstreamDigest = thirdUpstreamDigests.get("").getDigest();
    final String thirdTopLevelDownstreamDigest = thirdDownstreamDigests.get("").getDigest();

    assertEquals(thirdTopLevelUpstreamDigest, thirdTopLevelDownstreamDigest);
    */
  }

  private void sanityCheckDigests(String expectedKey, SortedMap<String, BucketDigest> upstreamDigests, SortedMap<String, BucketDigest> downstreamDigests) {
    assertNotNull(upstreamDigests);
    assertNotNull(downstreamDigests);

    assertTrue(upstreamDigests.containsKey(expectedKey));
    assertTrue(downstreamDigests.containsKey(expectedKey));
  }

  private <T extends PartitionedEvent> void insertAtRandomPoint(Random random, List<T> eventList, T event) {

    if (eventList.isEmpty()) {
      eventList.add(event);
    }
    else {
      int currentSize = eventList.size();
      int nextInsertion = random.nextInt(currentSize);
      eventList.add(nextInsertion, event);
    }
  }

  private class DatePartitionedEvent extends AbstractPartitionedEvent implements TestablePartitionedEvent {

    private final DateTimeFormatter YEARLY_FORMAT = DateTimeFormat.forPattern("yyyy");
    private final DateTimeFormatter MONTHLY_FORMAT = DateTimeFormat.forPattern("MM");
    private final DateTimeFormatter DAILY_FORMAT = DateTimeFormat.forPattern("dd");

    private DateTime date;

    private DatePartitionedEvent(String id, String version, Random random, String attributeName) {
      super(version, id);

      int range = 10;

      int randomDay = random.nextInt(range);
      DateTime start = new DateTime().minusDays(range);
      DateTime date = start.plusDays(randomDay);
      attributes.put(attributeName, date.toString());
      this.date = date;
    }

    @Override
    public MerkleNode getAttributeHierarchy() {
      MerkleNode leaf = new MerkleNode(DAILY_FORMAT.print(this.date), id, version);
      MerkleNode monthlyBucket = new MerkleNode(MONTHLY_FORMAT.print(this.date), leaf);
      return new MerkleNode(YEARLY_FORMAT.print(this.date), monthlyBucket);
    }
  }

  private class StringPartitionedEvent extends AbstractPartitionedEvent implements TestablePartitionedEvent {

    String attribute;

    private StringPartitionedEvent(String id, String version, String attributeName) {
      super(version, id);
      this.attribute = RandomStringUtils.randomAlphanumeric(10);
      attributes.put(attributeName, this.attribute);
    }

    @Override
    public MerkleNode getAttributeHierarchy() {
      return new MerkleNode(this.attribute.substring(0,2), id, version);
    }
  }

  private class EventStream implements Runnable {

    int cnt = 0;
    List<? extends PartitionedEvent> events;
    Long endpoint;
    VersionStore store;


    EventStream(VersionStore store, Long endpoint, List<? extends PartitionedEvent> events) {
      this.events = events;
      this.endpoint = endpoint;
      this.store = store;
    }

    @Override
    public void run() {
      for (PartitionedEvent event : events) {
        store.onEvent(endpoint, event);
      }
    }
  }

}

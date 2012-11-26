package net.lshift.diffa.versioning;

import com.ecyrd.speed4j.StopWatch;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import net.lshift.diffa.adapter.scanning.*;
import net.lshift.diffa.versioning.tables.records.ThingsRecord;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.util.*;

public class CassandraVersionStoreIT {

  static Logger log = LoggerFactory.getLogger(CassandraVersionStoreIT.class);

  @Test
  public void interviewShouldFinishWhenVersionStoreIsInSyncWithRemoteSystem() throws Exception {

    int maxSliceSize = 100;
    final Long left = System.currentTimeMillis() * 2;

    CassandraVersionStore store = new CassandraVersionStore();
    store.setMaxSliceSize(left, maxSliceSize);

    PartitionAwareStore partitionAwareStore = new PartitionAwareStore(left + "");

    String attributeName = "bizDate";

    // Populate the remote system with some random data

    Random random = new Random();
    DateTime median = new DateTime();
    int itemsInSync = 100;

    for (int i = 0; i < itemsInSync; i++) {
      DateTime randomDate = median.minusDays(random.nextInt(365 * 10));
      PartitionedEvent event = partitionAwareStore.createRandomThing(ImmutableMap.of(attributeName, randomDate));
      store.addEvent(left, event);
    }

    // Begin the interview process

    ScanAggregation dateAggregation = new DateAggregation(attributeName, DateGranularityEnum.Yearly);

    Set<ScanConstraint> cons = null;
    Set<ScanAggregation> aggs = ImmutableSet.of(dateAggregation);

    Set<ScanResultEntry> entries = partitionAwareStore.scanStore(cons, aggs, maxSliceSize);

    List<ScanRequest> requests = store.continueInterview(left, cons, aggs, entries);

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

    log.info("Initial (uncached) tree query");

    SortedMap<String,BucketDigest> upstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,BucketDigest> downstreamDigests = store.getEntityIdDigests(downstream);

    sanityCheckDigests("", upstreamDigests, downstreamDigests);

    final String firstTopLevelUpstreamDigest = upstreamDigests.get("").getDigest();
    final String firstTopLevelDownstreamDigest = downstreamDigests.get("").getDigest();

    assertEquals(firstTopLevelUpstreamDigest, firstTopLevelDownstreamDigest);

    List<EntityDifference> firstFlatComparison = store.flatComparison(upstream, downstream);
    assertTrue(firstFlatComparison.isEmpty());

    List<EntityDifference> firstIncrementalComparison = store.incrementalComparison(upstream, downstream);
    assertTrue(firstIncrementalComparison.isEmpty());

    log.info("Subsequent (cached) tree query");

    store.getEntityIdDigests(upstream);
    store.getEntityIdDigests(downstream);

    final TestablePartitionedEvent randomUpstreamEvent = upstreamEvents.get(random.nextInt(itemsInSync));
    randomUpstreamEvent.setVersion(RandomStringUtils.randomAlphanumeric(10));

    store.addEvent(upstream, randomUpstreamEvent);

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

    List<EntityDifference> secondDiffs = store.flatComparison(upstream, downstream);
    assertEquals(1, secondDiffs.size());

    EntityDifference difference = secondDiffs.get(0);
    assertEquals(randomUpstreamEvent.getId(), difference.getId());
    assertEquals(randomUpstreamEvent.getVersion(), difference.getUpstreamVersion());

    Predicate<TestablePartitionedEvent> filter = new Predicate<TestablePartitionedEvent>() {

      @Override
      public boolean apply(TestablePartitionedEvent input) {
        return input.getId().equals(randomUpstreamEvent.getId());
      }
    };

    TestablePartitionedEvent correspondingEvent = Iterables.find(downstreamEvents, filter);
    assertEquals(correspondingEvent.getVersion(), difference.getDownstreamVersion());

    String idToDelete = randomUpstreamEvent.getId();

    store.deleteEvent(upstream, idToDelete);
    store.deleteEvent(downstream, idToDelete);

    log.info("Tree query after upstream and downstream deletions (dirty cache)");

    SortedMap<String,BucketDigest> thirdUpstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,BucketDigest> thirdDownstreamDigests = store.getEntityIdDigests(downstream);

    sanityCheckDigests("", thirdUpstreamDigests, thirdDownstreamDigests);

    final String thirdTopLevelUpstreamDigest = thirdUpstreamDigests.get("").getDigest();
    final String thirdTopLevelDownstreamDigest = thirdDownstreamDigests.get("").getDigest();

    assertEquals(thirdTopLevelUpstreamDigest, thirdTopLevelDownstreamDigest);

    List<EntityDifference> thirdDiffs = store.flatComparison(upstream, downstream);
    assertTrue(thirdDiffs.isEmpty());

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

  private class DatePartitionedEvent extends AbstractPartitionedEvent {

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

  private class StringPartitionedEvent extends AbstractPartitionedEvent {

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
        store.addEvent(endpoint, event);
      }
    }
  }

}

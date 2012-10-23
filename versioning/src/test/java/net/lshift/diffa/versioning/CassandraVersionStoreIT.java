package net.lshift.diffa.versioning;

import com.ecyrd.speed4j.StopWatch;
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

  CassandraVersionStore store = new CassandraVersionStore();

  @Test
  public void shouldBeAbleToRoundTripChangeEvent() throws Exception {

    Random random = new Random();

    List<TestablePartitionedEvent> upstreamEvents = new LinkedList<TestablePartitionedEvent>();
    List<TestablePartitionedEvent> downstreamEvents = new LinkedList<TestablePartitionedEvent>();

    int itemsInSync = 2;

    for (int i = 0; i < itemsInSync; i++) {

      String id = RandomStringUtils.randomAlphanumeric(10);
      String version = RandomStringUtils.randomAlphanumeric(10);

      TestablePartitionedEvent upstreamEvent = new DatePartitionedEvent(id, version, random, "transactionDate" );
      TestablePartitionedEvent downstreamEvent = new StringPartitionedEvent(id, version, "userId");

      insertAtRandomPoint(random, upstreamEvents, upstreamEvent);
      insertAtRandomPoint(random, downstreamEvents, downstreamEvent);
    }

    final Long upstream = System.currentTimeMillis();
    final Long downstream = upstream + 1;

    Thread upstreamEventStream = new Thread(new EventStream(upstream, upstreamEvents));
    Thread downstreamEventStream = new Thread(new EventStream(downstream, downstreamEvents));

    upstreamEventStream.start();
    downstreamEventStream.start();

    upstreamEventStream.join();
    downstreamEventStream.join();

    long start = System.currentTimeMillis();

    SortedMap<String,String> upstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,String> downstreamDigests = store.getEntityIdDigests(downstream);

    long stop = System.currentTimeMillis();
    double time = stop - start;
    double rate = itemsInSync / time;

    log.info("Uncached tree comparison rate {}/ms", rate);

    sanityCheckDigests(upstream, downstream, upstreamDigests, downstreamDigests);

    final String firstTopLevelUpstreamDigest = upstreamDigests.get(upstream.toString());
    final String firstTopLevelDownstreamDigest = downstreamDigests.get(downstream.toString());

    assertEquals(firstTopLevelUpstreamDigest, firstTopLevelDownstreamDigest);

    start = System.currentTimeMillis();

    store.getEntityIdDigests(upstream);
    store.getEntityIdDigests(downstream);

    stop = System.currentTimeMillis();
    time = stop - start;
    rate = itemsInSync / time;

    log.info("Cached tree comparison rate {}/ms", rate);

    TestablePartitionedEvent randomUpstreamEvent = upstreamEvents.get(random.nextInt(itemsInSync));
    randomUpstreamEvent.setVersion(RandomStringUtils.randomAlphanumeric(10));

    store.addEvent(upstream, randomUpstreamEvent);

    start = System.currentTimeMillis();

    SortedMap<String,String> secondUpstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,String> secondDownstreamDigests = store.getEntityIdDigests(downstream);

    stop = System.currentTimeMillis();
    time = stop - start;
    rate = itemsInSync / time;

    log.info("Cached tree comparison rate after mutation {}/ms", rate);

    sanityCheckDigests(upstream, downstream, secondUpstreamDigests, secondDownstreamDigests);

    final String secondTopLevelUpstreamDigest = secondUpstreamDigests.get(upstream.toString());
    final String secondTopLevelDownstreamDigest = secondDownstreamDigests.get(downstream.toString());

    assertEquals(firstTopLevelDownstreamDigest, secondTopLevelDownstreamDigest);
    assertFalse(
      "1st and 2nd upstream digests should be different but were both " + firstTopLevelUpstreamDigest,
      firstTopLevelUpstreamDigest.equals(secondTopLevelUpstreamDigest)
    );

    String idToDelete = randomUpstreamEvent.getId();

    store.deleteEvent(upstream, idToDelete);
    store.deleteEvent(downstream, idToDelete);

    start = System.currentTimeMillis();

    SortedMap<String,String> thirdUpstreamDigests = store.getEntityIdDigests(upstream);
    SortedMap<String,String> thirdDownstreamDigests = store.getEntityIdDigests(downstream);

    stop = System.currentTimeMillis();
    time = stop - start;
    rate = itemsInSync / time;

    log.info("Cached tree comparison rate after mutation {}/ms", rate);

    sanityCheckDigests(upstream, downstream, thirdUpstreamDigests, thirdDownstreamDigests);

    final String thirdTopLevelUpstreamDigest = thirdUpstreamDigests.get(upstream.toString());
    final String thirdTopLevelDownstreamDigest = thirdDownstreamDigests.get(downstream.toString());

    assertEquals(thirdTopLevelUpstreamDigest, thirdTopLevelDownstreamDigest);


  }

  private void sanityCheckDigests(Long upstream, Long downstream, SortedMap<String, String> upstreamDigests, SortedMap<String, String> downstreamDigests) {
    assertNotNull(upstreamDigests);
    assertNotNull(downstreamDigests);

    assertTrue(upstreamDigests.containsKey(upstream.toString()));
    assertTrue(downstreamDigests.containsKey(downstream.toString()));
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

    List<? extends PartitionedEvent> events;
    Long endpoint;


    EventStream(Long endpoint, List<? extends PartitionedEvent> events) {
      this.events = events;
      this.endpoint = endpoint;
    }

    @Override
    public void run() {
      for (PartitionedEvent event : events) {
        store.addEvent(endpoint, event);
      }
    }
  }

}

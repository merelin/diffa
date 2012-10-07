package net.lshift.diffa.versioning.itest;

import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.adapter.scanning.DateAggregation;
import net.lshift.diffa.adapter.scanning.DateGranularityEnum;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.StringPrefixAggregation;
import net.lshift.diffa.versioning.CassandraVersionStore;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraVersionStoreIT {

  CassandraVersionStore store = new CassandraVersionStore();

  @Test
  public void shouldBeAbleToAddChangeEvent() {
    createRandomEvent();
  }

  private void loadTest() {
    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000 * 1000; i++) {
      createRandomEvent();
      if (i % 1000 == 0) {
        long stop = System.currentTimeMillis();
        System.err.println("Rate = " + (stop - start));
        start = System.currentTimeMillis();
      }
    }
  }

  private void createRandomEvent() {
    ChangeEvent ce = new ChangeEvent();
    ce.setId(RandomStringUtils.randomAlphanumeric(10));
    ce.setVersion(RandomStringUtils.randomAlphanumeric(6));

    Map<String,String> attributes = new HashMap<String,String>();

    attributes.put("foo" , new DateTime().minusDays(RandomUtils.nextInt(50)).toString());
    attributes.put("bar" , RandomStringUtils.randomAlphanumeric(12));
    ce.setAttributes(attributes);

    List<ScanAggregation> aggregations = new ArrayList<ScanAggregation>();
    aggregations.add(new DateAggregation("foo", DateGranularityEnum.Daily)) ;
    aggregations.add(new StringPrefixAggregation("foo", 2));

    store.store(0L, "ep", ce, aggregations);
  }

}

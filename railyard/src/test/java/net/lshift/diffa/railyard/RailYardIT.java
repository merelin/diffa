package net.lshift.diffa.railyard;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.events.SimpleUpsertEvent;
import net.lshift.diffa.events.UpsertEvent;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.*;

import static java.util.Arrays.*;

public class RailYardIT {

  @Test
  public void shouldPostChangeEvents() throws Exception {

    ChangeEvent event = new SimpleUpsertEvent(
        RandomStringUtils.randomAlphanumeric(10),
        RandomStringUtils.randomAlphanumeric(10),
        new DateTime()
    );

    RailYard railYard = new RailYardClient("http://localhost:7655");
    railYard.postChanges("space", "endpoint", asList(event));
  }

}

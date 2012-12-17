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
import static org.junit.Assert.*;

public class RailYardIT {

  RailYard railYard = new RailYardClient("http://localhost:7655");

  @Test
  public void shouldPostChangeEvents() throws Exception {

    ChangeEvent event = new SimpleUpsertEvent(
        RandomStringUtils.randomAlphanumeric(10),
        RandomStringUtils.randomAlphanumeric(10),
        new DateTime()
    );

    railYard.postChanges("space", "endpoint", asList(event));
  }

  @Test
  public void shouldGetNextQuestion() throws Exception {

    Question question = railYard.getNextQuestion("space", "endpoint");
    assertNotNull(question);

    Question nextQuestion = railYard.getNextQuestion("space", "endpoint", question, new Iterable<Answer>() {
      @Override
      public Iterator<Answer> iterator() {
        List<Answer> answers = new ArrayList<Answer>();
        return  answers.iterator();
      }
    });

    assertNotNull(nextQuestion);

  }

}

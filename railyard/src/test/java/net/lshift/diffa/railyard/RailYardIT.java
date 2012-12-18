package net.lshift.diffa.railyard;

import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.events.SimpleUpsertEvent;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;

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

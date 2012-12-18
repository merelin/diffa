package net.lshift.diffa.railyard;

import net.lshift.diffa.config.RangeCategoryDescriptor;
import net.lshift.diffa.system.Endpoint;
import net.lshift.diffa.system.SystemConfiguration;
import net.lshift.diffa.versioning.VersionStore;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class InterviewResourceTest {

  @Test
  public void shouldCreateInitialQuestion() throws Exception {

    String space = RandomStringUtils.randomAlphabetic(10);
    String endpointName = RandomStringUtils.randomAlphabetic(10);

    Endpoint endpoint = new Endpoint();
    endpoint.addCategory("some_date", new RangeCategoryDescriptor("date", "2000-01-01", "2010-01-01"));

    SystemConfiguration conf = mock(SystemConfiguration.class);
    VersionStore store = mock(VersionStore.class);

    when(conf.getEndpoint(space, endpointName)).thenReturn(endpoint);

    InterviewResource interview = new InterviewResource(store, conf);
    Question question = interview.getNextQuestion(space, endpointName);

    assertEquals(1, question.getAggregations());
    assertEquals(1, question.getConstraints());
  }
}

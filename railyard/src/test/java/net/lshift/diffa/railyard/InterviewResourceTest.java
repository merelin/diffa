package net.lshift.diffa.railyard;

import net.lshift.diffa.config.RangeCategoryDescriptor;
import net.lshift.diffa.scanning.Scannable;
import net.lshift.diffa.system.Endpoint;
import net.lshift.diffa.system.SystemConfiguration;
import net.lshift.diffa.versioning.VersionStore;
import org.apache.commons.lang.RandomStringUtils;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.resteasy.spi.HttpRequest;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class InterviewResourceTest {

  private SystemConfiguration conf = mock(SystemConfiguration.class);
  private VersionStore store = mock(VersionStore.class);
  private Scannable diffStore = mock(Scannable.class);

  private InterviewResource interview = new InterviewResource(store, conf, diffStore);

  private String space = RandomStringUtils.randomAlphabetic(10);
  private String endpointName = RandomStringUtils.randomAlphabetic(10);

  private Endpoint endpoint = new Endpoint();

  @Before
  public void setUp() {
    endpoint.addCategory("some_date", new RangeCategoryDescriptor("date", "2000-01-01", "2010-01-01"));
    when(conf.getEndpoint(space, endpointName)).thenReturn(endpoint);
  }

  @Test
  public void shouldCreateInitialQuestion() throws Exception {


    Question question = interview.getNextQuestion(space, endpointName);

    assertEquals(1, question.getAggregations().size());
    assertEquals(1, question.getConstraints().size());

  }

  @Test
  public void shouldCreateNextQuestion() throws Exception {

    MultivaluedMap map = new MultivaluedMapImpl();
    HttpRequest request = mock(HttpRequest.class);

    UriInfo uriInfo = mock(UriInfo.class);

    when(uriInfo.getQueryParameters()).thenReturn(map);
    when(request.getUri()).thenReturn(uriInfo);

    Question question = interview.getNextQuestion(space, endpointName, request);

    assertNotNull(question);
  }
}

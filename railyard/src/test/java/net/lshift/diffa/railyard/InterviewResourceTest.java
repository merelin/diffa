package net.lshift.diffa.railyard;

import net.lshift.diffa.system.SystemConfiguration;
import net.lshift.diffa.versioning.VersionStore;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class InterviewResourceTest {

  @Test
  public void shouldDoSomething() throws Exception {

    String space = RandomStringUtils.randomAlphabetic(10);
    String endpoint = RandomStringUtils.randomAlphabetic(10);

    SystemConfiguration conf = mock(SystemConfiguration.class);
    VersionStore store = mock(VersionStore.class);

    InterviewResource interview = new InterviewResource(store, conf);
    Question question = interview.getNextQuestion(space, endpoint);

    assertNotNull(question);
  }
}

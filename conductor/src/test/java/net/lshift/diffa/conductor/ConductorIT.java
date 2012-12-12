package net.lshift.diffa.conductor;

import org.junit.Test;

public class ConductorIT {

  static String url = "http://localhost:" + ConductorDaemon.DEFAULT_PORT;

  @Test
  public void shouldConductInterview() throws Exception {

    Conductor conductor = new ConductorClient(url);
    //conductor.begin();

  }
}

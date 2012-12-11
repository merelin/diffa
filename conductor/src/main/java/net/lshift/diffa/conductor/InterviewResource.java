package net.lshift.diffa.conductor;

import com.google.inject.Inject;
import net.lshift.diffa.railyard.RailYard;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/{space}/interview")
public class InterviewResource {

  @Inject
  private RailYard railyard;

  @GET
  public void begin() {

  }
}

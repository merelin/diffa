package net.lshift.diffa.conductor;

import com.google.inject.Inject;
import net.lshift.diffa.railyard.RailYard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("/{space}/interview")
public class InterviewResource {

  static Logger log = LoggerFactory.getLogger(InterviewResource.class);

  @Inject
  private RailYard railyard;

  @POST
  @Path("/{endpoint}")
  public void begin(@PathParam("endpoint") String endpoint, @PathParam("space") String space) {
    log.info("Kicking of interview for {} endpoint in space {}", endpoint, space);
  }
}

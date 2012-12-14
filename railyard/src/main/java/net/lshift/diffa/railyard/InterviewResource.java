package net.lshift.diffa.railyard;

import net.lshift.diffa.adapter.scanning.HttpRequestParameters;
import net.lshift.diffa.adapter.scanning.SliceSizeParser;
import net.lshift.diffa.railyard.plumbing.RestEasyRequestWrapper;
import org.jboss.resteasy.spi.HttpRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import java.io.IOException;
import java.net.URI;

@Path("/{endpoint}/interview")
public class InterviewResource {

  @GET
  @Produces("application/json")
  public Question getNextQuestion(@PathParam("endpoint") Long endpoint) {
    return new SimpleQuestion();
  }

  @POST
  @Produces("application/json")
  public Question getNextQuestion(@PathParam("endpoint") Long endpoint,
                                  @Context final HttpRequest request) throws IOException {

    HttpRequestParameters parameters = new RestEasyRequestWrapper(request);
    SliceSizeParser sliceSizeParser = new SliceSizeParser(parameters);


    return new SimpleQuestion();
  }
}

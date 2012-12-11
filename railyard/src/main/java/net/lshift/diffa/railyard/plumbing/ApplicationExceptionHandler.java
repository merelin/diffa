package net.lshift.diffa.railyard.plumbing;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class ApplicationExceptionHandler implements ExceptionMapper<Throwable> {

  @Override
  public Response toResponse(Throwable e) {
    return Response.status(500).entity("Unknown issue, please contact support").build();
  }
}

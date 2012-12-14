package net.lshift.diffa.auth;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class AuthenticatingTranscoder extends SimpleChannelUpstreamHandler {

  static Logger log = LoggerFactory.getLogger(AuthenticatingTranscoder.class);

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

    if (e.getMessage() instanceof HttpRequest) {

      HttpRequest request = (HttpRequest) e.getMessage();
      String space = extractSpace(request);

      if (isAuthenticated(space)) {

        rewriteUri(request, System.currentTimeMillis());
        ctx.sendUpstream(e);

      }
      else {

        ctx.getPipeline().addLast("encoder", new HttpResponseEncoder());
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, FORBIDDEN);
        response.setHeader(CONTENT_LENGTH, 0);
        ChannelFuture future = ctx.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);

      }

    }
    else if (e.getMessage() instanceof HttpChunk) {

      // TODO This is very dodgy - basically we're assuming that the input stream was pre-validated .... fix this

      ctx.sendUpstream(e);

    }
    else {
      log.error("Forgot to implement some code: " + e.getMessage());
    }



  }

  private void rewriteUri(HttpRequest request, Long endpoint) {
    String url;
    String original = request.getUri();
    int index = original.indexOf("?");

    if (index > 0) {
      String query = original.substring(index);
      url = String.format("/%s/interview%s", endpoint, query);
    }
    else {
      url = String.format("/%s/interview", endpoint);
    }

    request.setUri(url);
  }

  private String extractSpace(HttpRequest request) {
    return "space";
  }

  private boolean isAuthenticated(String space) {
    return true;
  }
}

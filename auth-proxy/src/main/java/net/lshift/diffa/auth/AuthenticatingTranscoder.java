package net.lshift.diffa.auth;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class AuthenticatingTranscoder extends SimpleChannelUpstreamHandler {

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

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

  private void rewriteUri(HttpRequest request, Long endpoint) {
    String url = String.format("/%s/interview", endpoint);
    request.setUri(url);
  }

  private String extractSpace(HttpRequest request) {
    return "space";
  }

  private boolean isAuthenticated(String space) {
    return true;
  }
}

package net.lshift.diffa.conductor;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;

public class ConductorClient implements Conductor {

  private AsyncHttpClient client = new AsyncHttpClient();
  private String baseUrl;

  @Inject
  public ConductorClient(@Named("conductorUrl") String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @Override
  public void begin(String space, String endpoint) {

    String url = baseUrl + String.format("/%s/interview/%s", space, endpoint);

    Request request
        = new RequestBuilder("POST").
          setUrl(url).
          build();

    try {

      Response response = client.prepareRequest(request).execute().get();
      verifyResponse(response, 204);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void verifyResponse(Response response, int code) {
    if (response.getStatusCode() != code) {
      switch (response.getStatusCode()) {
        default: throw new ConductorException("HTTP " + response.getStatusCode() + " : " + response.getStatusText());
      }
    }
  }
}

package net.lshift.diffa.conductor;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ning.http.client.*;
import com.ning.http.client.generators.ByteArrayBodyGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class ConductorClient implements Conductor {

  private ObjectMapper mapper = new ObjectMapper();
  private AsyncHttpClient client = new AsyncHttpClient();
  private String baseUrl;

  @Inject
  public ConductorClient(@Named("conductorUrl") String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @Override
  public void registerDriver(String space, String endpoint, DriverConfiguration configuration) {

    BodyGenerator body = getSerializedPayload(configuration);
    String url = baseUrl + String.format("/%s/interview/%s/config", space, endpoint);

    Request request
        = new RequestBuilder("POST").
        setUrl(url).
        setBody(body).
        setHeader("Content-Type", "application/json").
        build();

    try {

      Response response = client.prepareRequest(request).execute().get();
      verifyResponse(response, 204);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  private BodyGenerator getSerializedPayload(Object o) {

    byte[] payload = null;

    try {
      payload = mapper.writeValueAsBytes(o);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new ByteArrayBodyGenerator(payload);
  }

  private void verifyResponse(Response response, int code) {
    if (response.getStatusCode() != code) {
      switch (response.getStatusCode()) {
        default: throw new ConductorException("HTTP " + response.getStatusCode() + " : " + response.getStatusText());
      }
    }
  }
}

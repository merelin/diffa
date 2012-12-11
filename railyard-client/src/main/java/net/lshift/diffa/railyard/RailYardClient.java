package net.lshift.diffa.railyard;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ning.http.client.*;
import com.ning.http.client.generators.InputStreamBodyGenerator;
import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.railyard.plumbing.ChangeEventPipe;
import org.codehaus.jackson.JsonFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RailYardClient implements RailYard {

  private JsonFactory factory = new JsonFactory();
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private AsyncHttpClient client = new AsyncHttpClient();
  private String baseUrl;

  @Inject
  public RailYardClient(@Named("railYardUrl") String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public void postChanges(String space, String endpoint, Iterable<ChangeEvent> events) {

    if (events == null || !events.iterator().hasNext() ) {
      throw new RailYardException("Client must supply at least one change event");
    }

    ChangeEventPipe changeEventPipe = new ChangeEventPipe(events, executorService, factory);
    InputStreamBodyGenerator encoder = new InputStreamBodyGenerator(changeEventPipe);

    String url = baseUrl + String.format("/%s/changes/%s", space, endpoint);
    Request request =
        new RequestBuilder("POST").
          setUrl(url).
          setBody(encoder).
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
        default: throw new RailYardException("HTTP " + response.getStatusCode() + " : " + response.getStatusText());
      }
    }
  }
}

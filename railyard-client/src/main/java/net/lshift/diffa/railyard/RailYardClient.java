package net.lshift.diffa.railyard;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.ning.http.client.*;
import com.ning.http.client.generators.InputStreamBodyGenerator;
import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.plumbing.BufferedJsonBodyConsumer;
import net.lshift.diffa.railyard.plumbing.*;
import net.lshift.diffa.scanning.http.ScanRequestEncoder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RailYardClient implements RailYard {

  private ObjectMapper mapper = new ObjectMapper();
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

    EventPipe eventPipe = new ChangeEventPipe(events, executorService, factory);
    InputStreamBodyGenerator encoder = new InputStreamBodyGenerator(eventPipe);

    final String url = baseUrl + String.format("/%s/changes/%s", space, endpoint);

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

  @Override
  public Question getNextQuestion(String space, String endpoint) {

    Question question = null;

    final String url = baseUrl + String.format("/%s/interview/%s", space, endpoint);

    SimpleAsyncHttpClient httpClient = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .build();

    BufferedJsonBodyConsumer consumer = new BufferedJsonBodyConsumer();

    try {

      Response response = httpClient.get(consumer).get();
      verifyResponse(response, 200);
      question = consumer.getValue(Question.class);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return question;
  }

  @Override
  public Question getNextQuestion(String space, String endpoint, Question lastQuestion, Iterable<Answer> answers) {

    Question question = null;

    final String url = baseUrl + String.format("/%s/interview/%s", space, endpoint);

    FluentStringsMap queryParameters = ScanRequestEncoder.packRequest(lastQuestion.getConstraints(), lastQuestion.getAggregations(), lastQuestion.getMaxSliceSize());

    EventPipe eventPipe = new AnswerEventPipe(answers, executorService, factory);
    InputStreamBodyGenerator encoder = new InputStreamBodyGenerator(eventPipe);

    Request request =
        new RequestBuilder("POST").
            setUrl(url).
            setBody(encoder).
            setQueryParameters(queryParameters).
            build();

    try {

      Response response = client.prepareRequest(request).execute().get();
      verifyResponse(response, 200);

      byte[] payload = response.getResponseBodyAsBytes();
      question = mapper.readValue(payload, Question.class);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return question;
  }

  private void verifyResponse(Response response, int code) {
    if (response.getStatusCode() != code) {
      switch (response.getStatusCode()) {
        default: throw new RailYardException("HTTP " + response.getStatusCode() + " : " + response.getStatusText());
      }
    }
  }
}

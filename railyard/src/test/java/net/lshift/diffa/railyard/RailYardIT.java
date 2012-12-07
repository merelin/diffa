package net.lshift.diffa.railyard;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.junit.Test;

public class RailYardIT {

  @Test
  public void doit() throws Exception {
    AsyncHttpClient client = new AsyncHttpClient();
    Response r = client.preparePost("http://localhost:7655/s1/changes/jd").execute().get();
  }

}

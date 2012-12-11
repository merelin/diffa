package net.lshift.diffa.railyard.plumbing;

import net.lshift.diffa.events.ChangeEvent;
import net.lshift.diffa.events.UpsertEvent;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

public class ChangeEventPipe extends InputStream {

  private final PipedInputStream is;

  public ChangeEventPipe(Iterable<ChangeEvent> events, ExecutorService executorService, JsonFactory factory) {
    final Iterator<ChangeEvent> iterator = events.iterator();
    final PipedOutputStream os = new PipedOutputStream();

    try {
      this.is = new PipedInputStream(os);

      final JsonGenerator generator = factory.createJsonGenerator(os);

      executorService.execute(new Runnable() {

        @Override
        public void run() {
          try {
            generator.writeStartArray();

            while (iterator.hasNext()) {
              ChangeEvent event = iterator.next();
              generator.writeStartObject();
              generator.writeStringField("id", event.getId());

              if (event instanceof UpsertEvent) {
                UpsertEvent upsert = (UpsertEvent) event;
                generator.writeStringField("version", upsert.getVersion());
              }

              generator.writeEndObject();
              generator.flush();
            }

            generator.writeEndArray();
            generator.flush();

            os.flush();
            os.close();

          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public int read() throws IOException {
    return is.read();
  }
}

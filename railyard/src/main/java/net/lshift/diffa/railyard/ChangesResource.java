package net.lshift.diffa.railyard;

import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.versioning.CassandraVersionStore;
import net.lshift.diffa.versioning.partitioning.PartitionedEvent;
import net.lshift.diffa.versioning.VersionStore;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.jboss.resteasy.spi.BadRequestException;
import org.jboss.resteasy.spi.HttpRequest;
import org.joda.time.DateTime;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Path("/changes")
public class ChangesResource {


  // Thread safe factory
  private static JsonFactory factory = new MappingJsonFactory();

  // Thread safe versionStore (for now, or at least until this is no longer true)
  private static VersionStore versionStore = new CassandraVersionStore();



  @POST
  @Path("/{endpoint}")
  public void onChange(@PathParam("endpoint") Long endpoint, @Context HttpRequest request) throws IOException {

    InputStream is = request.getInputStream();
    JsonParser parser = factory.createJsonParser(is);

    JsonToken current = parser.nextToken();

    if (current == JsonToken.START_OBJECT) {

      ChangeEvent event = parseEvent(parser);
      handleChangeEvent(endpoint, event);

    }
    else if (current == JsonToken.START_ARRAY) {

      while ((parser.nextToken()) != JsonToken.END_ARRAY) {

        ChangeEvent event = parseEvent(parser);
        handleChangeEvent(endpoint, event);

      }
    }
    else {
      throw new BadRequestException("Malformed JSON input");
    }
  }

  private void handleChangeEvent(Long endpoint, ChangeEvent event) {
    if (event.getVersion() == null) {
      versionStore.deleteEvent(endpoint, event.getId());
    } else {
      PartitionedEvent partitionedEvent = partitionEvent(event);
      versionStore.addEvent(endpoint, partitionedEvent);
    }
  }

  private static PartitionedEvent partitionEvent(ChangeEvent event) {
    PartitionedEvent partitionedEvent =
    return null;
  }

  private static ChangeEvent parseEvent(JsonParser parser) throws IOException {

    ChangeEvent event = new ChangeEvent();

    JsonNode node = parser.readValueAsTree();

    checkMandatoryAttribute(node, "id");
    event.setId(node.get("id").getTextValue());

    if (node.has("version")) {
      event.setVersion(node.get("version").getTextValue());
    }

    // TODO This is hardcoded
    event.setLastUpdated(new DateTime());

    if (node.has("attributes")) {

      Map<String,String> attributes = new HashMap<String, String>();
      event.setAttributes(attributes);

      Iterator<Map.Entry<String, JsonNode>> fields = node.get("attributes").getFields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        attributes.put(entry.getKey(), entry.getValue().getTextValue());
      }

    }


    return event;
  }

  private static void checkMandatoryAttribute(JsonNode node, String attribute) {
    if (!node.has(attribute)) {
      throw new BadRequestException("Missing mandatory attribute: " + attribute);
    }
  }
}

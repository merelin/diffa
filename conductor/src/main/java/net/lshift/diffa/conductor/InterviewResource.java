package net.lshift.diffa.conductor;

import com.google.inject.Inject;
import com.jolbox.bonecp.BoneCPDataSource;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.conductor.plumbing.InterviewResultHandler;
import net.lshift.diffa.railyard.NoFurtherQuestions;
import net.lshift.diffa.railyard.Question;
import net.lshift.diffa.railyard.RailYard;
import net.lshift.diffa.scanning.Scannable;
import net.lshift.diffa.sql.PartitionAwareDriver;
import net.lshift.diffa.sql.PartitionMetadata;
import org.jboss.resteasy.spi.BadRequestException;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.ws.rs.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static net.lshift.diffa.conductor.plumbing.ConfigurationBuilder.*;

@Path("/{space}/interview")
public class InterviewResource {

  static Logger log = LoggerFactory.getLogger(InterviewResource.class);

  @Inject
  private RailYard railyard;

  private Map<String, Scannable> drivers = new ConcurrentHashMap<String, Scannable>();

  private ExecutorService executor = Executors.newCachedThreadPool();

  @POST
  @Path("/{endpoint}/config")
  @Consumes("application/json")
  public void registerDriver(@PathParam("space") String space, @PathParam("endpoint") String endpoint, DriverConfiguration config) {

    DataSource ds = buildDataSource(config);
    PartitionMetadata metadata = buildMetaData(config);

    PartitionAwareDriver driver = new PartitionAwareDriver(ds, metadata);
    registerDriver(space, endpoint, driver);
  }

  @POST
  @Path("/{endpoint}")
  public void begin(@PathParam("space") String space, @PathParam("endpoint") String endpoint) {

    Scannable driver = getDriver(space, endpoint);

    if (driver == null) {
      throw new BadRequestException("No driver registered for " + space + " / " + endpoint);
    }

    log.info("Kicking of interview for {} endpoint in space {}", endpoint, space);

    Interview interview = new Interview(railyard, driver, space, endpoint);
    executor.execute(interview);

  }

  private Scannable getDriver(String space, String endpoint) {
    return drivers.get(space + "." + endpoint);
  }

  private void registerDriver(String space, String endpoint, Scannable driver) {
    drivers.put(space + "." + endpoint, driver);
  }



  private class Interview implements Runnable {

    private final RailYard railyard;
    private final Scannable scannable;
    private final String space, endpoint;

    private Interview(RailYard railyard, Scannable scannable, String space, String endpoint) {
      this.railyard = railyard;
      this.scannable = scannable;
      this.space = space;
      this.endpoint = endpoint;
    }

    @Override
    public void run() {

      Question question = railyard.getNextQuestion(space, endpoint);

      while ( !(question instanceof NoFurtherQuestions) ) {

        Set<ScanConstraint> constraints = question.getConstraints();
        Set<ScanAggregation> aggregations = question.getAggregations();
        int maxSliceSize = question.getMaxSliceSize();

        InterviewResultHandler handler = new InterviewResultHandler();

        scannable.scan(constraints, aggregations, maxSliceSize, handler);

        question = railyard.getNextQuestion(space, endpoint, question, handler);

      }

    }
  }
}

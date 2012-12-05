package net.lshift.diffa.agent.rest

import scala.collection.JavaConversions._
import javax.ws.rs.{Produces, Path, GET}
import javax.ws.rs.core.{StreamingOutput, Response, Context}
import javax.servlet.http.HttpServletRequest
import net.lshift.diffa.adapter.scanning.{SliceSizeParser, ScanResultEntry, AggregationBuilder, ConstraintsBuilder}
import org.springframework.stereotype.Component
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.beans.factory.annotation.Autowired
import net.lshift.diffa.kernel.differencing.DomainDifferenceStore
import java.io.{BufferedOutputStream, OutputStream}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import net.lshift.diffa.scanning.ScanResultHandler
import net.lshift.diffa.adapter.avro.{ScanResultEntry => GeneratedScanResultEntry}


@Path("/store")
@Component
@PreAuthorize("hasRole('root')")
class StoreSynchronizationResource {

  @Autowired var diffStore:DomainDifferenceStore = null

  @GET
  @Path("/scan/{extent}")
  @Produces(Array("application/json"))
  def scanPairs(@Context request:HttpServletRequest) = {

    //def generateVersion(domain:String) = ScannableUtils.generateDigest(domain)


    val constraintsBuilder = new ConstraintsBuilder(request)
    constraintsBuilder.maybeAddStringPrefixConstraint("name")
    val constraints = constraintsBuilder.toSet

    val aggregationsBuilder = new AggregationBuilder(request)
    aggregationsBuilder.maybeAddStringPrefixAggregation("entityId")
    val aggregations = aggregationsBuilder.toSet

    val sliceParser = new SliceSizeParser(request)
    val maxSliceSize = sliceParser.getMaxSliceSize

    val stream = new StreamingOutput {
      def write(os: OutputStream) {

        val encoder = EncoderFactory.get().binaryEncoder(os, null)

        // We're using this indirection (GeneratedScanResultEntry) at the moment so as to not put a direct dependency on avro
        // in the kernel module - should this code ever stabilize over time, we can think about getting rid of the
        // indirection

        val writer = new SpecificDatumWriter[GeneratedScanResultEntry](classOf[GeneratedScanResultEntry]);
        writer.setSchema(GeneratedScanResultEntry.SCHEMA$);

        val handler = new ScanResultHandler {
          def onEntry(entry: ScanResultEntry) {
            val generated = new GeneratedScanResultEntry
            if (entry.getId != null) {
              generated.setId(entry.getId)
            }
            if (entry.getVersion != null) {
              generated.setVersion(entry.getVersion)
            }
            if (entry.getLastUpdated != null) {
              generated.setLastUpdated(entry.getLastUpdated.getMillis)
            }
            if (entry.getAttributes != null) {

              // If anybody can tell me why Scala needs to do this kind of thing, I'd like to know

              val asString = entry.getAttributes
              val asCharSequence = asString.map{ case (k,v) => (k.asInstanceOf[CharSequence],v.asInstanceOf[CharSequence])}
              generated.setAttributes(asCharSequence)
            }

            writer.write(generated, encoder)
            encoder.flush()
          }
        }

        diffStore.scan(constraints, aggregations, maxSliceSize, handler)

      }
    }

    Response.ok(stream).build()

  }

}

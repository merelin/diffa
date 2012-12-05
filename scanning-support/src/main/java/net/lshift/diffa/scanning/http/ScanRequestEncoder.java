package net.lshift.diffa.scanning.http;

import com.ning.http.client.FluentStringsMap;
import net.lshift.diffa.adapter.scanning.GranularityAggregation;
import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.StringPrefixAggregation;

import java.util.Set;

public class ScanRequestEncoder {

  public static FluentStringsMap packRequest(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize) {
    FluentStringsMap queryParams = new FluentStringsMap();

    for (ScanAggregation aggregation : aggregations) {

      if (aggregation instanceof StringPrefixAggregation) {
        StringPrefixAggregation spf = (StringPrefixAggregation) aggregation;
        queryParams.add(spf.getAttributeName() + "-length", spf.getLength() + "");
      }
      else if (aggregation instanceof GranularityAggregation) {
        GranularityAggregation ga = (GranularityAggregation) aggregation;
        queryParams.add(ga.getAttributeName() + "-granularity", ga.getGranularityString());
      }

    }

    for (ScanConstraint constraint : constraints) {
      // TODO implement
    }

    queryParams.add("max-slice-size", maxSliceSize + "");

    return queryParams;
  }


}

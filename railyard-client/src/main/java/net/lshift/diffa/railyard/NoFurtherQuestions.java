package net.lshift.diffa.railyard;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NoFurtherQuestions implements Question {
  @Override
  public Set<ScanConstraint> getConstraints() {
    return null;
  }

  @Override
  public Set<ScanAggregation> getAggregations() {
    return null;
  }

  @Override
  public int getMaxSliceSize() {
    return 0;
  }
}

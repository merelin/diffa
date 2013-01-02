package net.lshift.diffa.interview;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NoFurtherQuestions implements Question {

  public static final Question NO_FURTHER_QUESTIONS = new NoFurtherQuestions();

  public static Question get() {
    return NO_FURTHER_QUESTIONS;
  }

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

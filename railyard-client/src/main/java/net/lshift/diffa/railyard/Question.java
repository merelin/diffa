package net.lshift.diffa.railyard;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;

import java.util.Set;

public interface Question {

  Set<ScanConstraint> getConstraints();

  Set<ScanAggregation> getAggregations();

  int getMaxSliceSize();
}

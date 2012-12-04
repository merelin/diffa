package net.lshift.diffa.sql;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;

import java.util.Set;

public interface PartitionAwareDriver {

  void scanStore(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize, ScanResultHandler handler);

}

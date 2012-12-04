package net.lshift.diffa.scanning;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;

import java.util.Set;

/**
 * This is a java re-implementation of <code>ScanningParticipantRef</code>
 */
public interface Scannable {

  Set<ScanResultEntry> scan(Set<ScanConstraint> constraints, Set<ScanAggregation> aggregations, int maxSliceSize);
}

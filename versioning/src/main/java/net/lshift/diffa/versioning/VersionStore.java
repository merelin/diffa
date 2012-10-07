package net.lshift.diffa.versioning;

import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.adapter.scanning.ScanAggregation;

public interface VersionStore {

  public void store(Long space, String endpoint, ChangeEvent event, Iterable<ScanAggregation> aggregations);
}

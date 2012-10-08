package net.lshift.diffa.versioning;

import net.lshift.diffa.adapter.changes.ChangeEvent;
import net.lshift.diffa.adapter.scanning.ScanAggregation;

public interface VersionStore {

  public void addEvent(Long space, String endpoint, ChangeEvent event, Iterable<ScanAggregation> aggregations);
  public void deleteEvent(Long space, String endpoint, String id);

}
package net.lshift.diffa.versioning;

import net.lshift.diffa.adapter.scanning.ScanAggregation;
import net.lshift.diffa.adapter.scanning.ScanConstraint;
import net.lshift.diffa.adapter.scanning.ScanRequest;
import net.lshift.diffa.adapter.scanning.ScanResultEntry;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

public interface VersionStore {

  void addEvent(Long endpoint, PartitionedEvent event);
  void deleteEvent(Long endpoint, String id);
  
  SortedMap<String,BucketDigest> getEntityIdDigests(Long endpoint, String bucketName);
  SortedMap<String, BucketDigest> getEntityIdDigests(Long endpoint);

  SortedMap<String,BucketDigest> getUserDefinedDigests(Long endpoint, String bucketName, int maxSliceSize);
  SortedMap<String, BucketDigest> getUserDefinedDigests(Long endpoint, int maxSliceSize);

  List<EntityDifference> flatComparison(Long left, Long right);

  @Deprecated List<EntityDifference> incrementalComparison(Long left, Long right);

  // New API

  List<ScanRequest> continueInterview(Long endpoint,
                                      Set<ScanConstraint> constraints,
                                      Set<ScanAggregation> aggregations,
                                      Set<ScanResultEntry> entries);


  void setMaxSliceSize(Long endpoint, int size);
}
package net.lshift.diffa.versioning;

import java.util.List;
import java.util.SortedMap;

public interface VersionStore {

  void addEvent(Long endpoint, PartitionedEvent event);
  void deleteEvent(Long endpoint, String id);
  
  SortedMap<String,BucketDigest> getEntityIdDigests(Long endpoint, String bucketName);
  SortedMap<String, BucketDigest> getEntityIdDigests(Long endpoint);

  SortedMap<String,BucketDigest> getUserDefinedDigests(Long endpoint, String bucketName);
  SortedMap<String, BucketDigest> getUserDefinedDigests(Long endpoint);

  List<EntityDifference> flatComparison(Long left, Long right);
  List<EntityDifference> incrementalComparison(Long left, Long right);

}
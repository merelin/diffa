package net.lshift.diffa.versioning;

import java.util.SortedMap;

public interface VersionStore {

  public void addEvent(Long space, String endpoint, PartitionedEvent event);
  public void deleteEvent(Long space, String endpoint, String id);
  
  public SortedMap<String,String> getEntityIdDigests(Long space, String endpoint, String bucketName);
  public SortedMap<String,String> getEntityIdDigests(Long space, String endpoint);

  public SortedMap<String,String> getUserDefinedDigests(Long space, String endpoint, String bucketName);
  public SortedMap<String,String> getUserDefinedDigests(Long space, String endpoint);
  
}
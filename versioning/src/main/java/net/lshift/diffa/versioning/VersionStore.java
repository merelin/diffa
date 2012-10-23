package net.lshift.diffa.versioning;

import java.util.SortedMap;

public interface VersionStore {

  public void addEvent(Long endpoint, PartitionedEvent event);
  public void deleteEvent(Long endpoint, String id);
  
  public SortedMap<String,String> getEntityIdDigests(Long endpoint, String bucketName);
  public SortedMap<String, String> getEntityIdDigests(Long endpoint);

  public SortedMap<String,String> getUserDefinedDigests(Long endpoint, String bucketName);
  public SortedMap<String, String> getUserDefinedDigests(Long endpoint);
  
}
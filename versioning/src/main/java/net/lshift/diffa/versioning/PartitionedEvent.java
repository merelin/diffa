package net.lshift.diffa.versioning;

import org.joda.time.DateTime;

import java.util.Map;

public interface PartitionedEvent {

  String getId();
  String getVersion();
  DateTime getLastUpdated();
  MerkleNode getIdHierarchy();
  Map<String, String> getAttributes();
  MerkleNode getAttributeHierarchy();

}

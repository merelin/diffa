package net.lshift.diffa.versioning;

import net.lshift.diffa.versioning.partitioning.PartitionedEvent;

public interface TestablePartitionedEvent extends PartitionedEvent {

  void setVersion(String version);
}

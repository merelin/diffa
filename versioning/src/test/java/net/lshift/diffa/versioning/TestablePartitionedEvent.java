package net.lshift.diffa.versioning;

import net.lshift.diffa.versioning.events.PartitionedEvent;

public interface TestablePartitionedEvent extends PartitionedEvent {

  void setVersion(String version);
}

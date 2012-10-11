package net.lshift.diffa.versioning;

public interface TestablePartitionedEvent extends PartitionedEvent {

  void setVersion(String version);
}

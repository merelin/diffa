package net.lshift.diffa.versioning.events;

import net.lshift.diffa.versioning.partitioning.MerkleNode;
import net.lshift.diffa.versioning.partitioning.MerkleUtils;
import org.joda.time.DateTime;

public class DefaultUnpartitionedEvent implements UnpartitionedEvent {

  private final String id, version;
  private final DateTime lastUpdated;
  private final MerkleNode idHierarchy;

  public DefaultUnpartitionedEvent(String id, String version, DateTime lastUpdated) {
    this.id = id;
    this.version = version;
    this.lastUpdated = lastUpdated;
    idHierarchy = MerkleUtils.buildEntityIdNode(id, version);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public DateTime getLastUpdated() {
    return lastUpdated;
  }

  @Override
  public MerkleNode getIdHierarchy() {
    return idHierarchy;
  }
}

package net.lshift.diffa.versioning;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractPartitionedEvent implements TestablePartitionedEvent {

  protected String id;
  protected String version;
  protected DateTime lastUpdate = new DateTime();
  protected Map<String,String> attributes = new HashMap<String, String>();

  public AbstractPartitionedEvent(String version, String id) {
    this.version = version;
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public DateTime getLastUpdated() {
    return lastUpdate;
  }

  @Override
  public MerkleNode getIdHierarchy() {
    MerkleNode leaf = new MerkleNode(this.id.substring(4,6), id, version);
    MerkleNode mid = new MerkleNode(this.id.substring(2,4), leaf);
    return new MerkleNode(this.id.substring(0,2), mid);
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }
}

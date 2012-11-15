package net.lshift.diffa.versioning;

public class EntityDifference {
  private String id;
  private String upstreamVersion;
  private String downstreamVersion;

  public EntityDifference(String id, String upstreamVersion, String downstreamVersion) {
    this.id = id;
    this.upstreamVersion = upstreamVersion;
    this.downstreamVersion = downstreamVersion;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getUpstreamVersion() {
    return upstreamVersion;
  }

  public void setUpstreamVersion(String upstreamVersion) {
    this.upstreamVersion = upstreamVersion;
  }

  public String getDownstreamVersion() {
    return downstreamVersion;
  }

  public void setDownstreamVersion(String downstreamVersion) {
    this.downstreamVersion = downstreamVersion;
  }

  public boolean isDifferent() {

    if (upstreamVersion == null && downstreamVersion == null) {
      return false;
    }
    else if (upstreamVersion != null && downstreamVersion == null) {
      return true;
    }
    else if (upstreamVersion == null && downstreamVersion != null) {
      return true;
    }
    else {
      return !upstreamVersion.equals(downstreamVersion);
    }

  }
}

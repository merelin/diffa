package net.lshift.diffa.versioning;

import java.util.Map;

public class TreeLevelRollup {

  private Map<String,BucketDigest> members;
  private boolean isLeaf = false;

  public TreeLevelRollup(Map<String, BucketDigest> members, boolean leaf) {
    this.members = members;
    isLeaf = leaf;
  }

  public Map<String, BucketDigest> getMembers() {
    return members;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  @Override
  public String toString() {
    return "TreeLevelRollup{" +
        "members=" + members +
        ", isLeaf=" + isLeaf +
        '}';
  }

  public boolean isEmpty() {
    return members == null || members.isEmpty();
  }
}

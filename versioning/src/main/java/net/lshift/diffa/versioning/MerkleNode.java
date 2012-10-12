package net.lshift.diffa.versioning;

public class MerkleNode {

  private MerkleNode child;
  private String id;
  private String version;
  private String name;
  private boolean isLeaf = false;

  public MerkleNode(String name) {
    this.name = name;
  }

  public MerkleNode(String name, MerkleNode child) {
    this.name = name;
    this.child = child;
  }

  public MerkleNode(String name, String id, String version) {
    this.name = name;
    this.id = id;
    this.version = version;
    isLeaf = true;
  }

  // TODO quick and dirty path construction .....
  public String getDescendencyPath() {
    if (isLeaf()) {
      return getName();
    }
    else {
      return getName() + "." + getChild().getDescendencyPath();
    }
  }

  public MerkleNode getChild() {
    return child;
  }

  public void setChild(MerkleNode child) {
    this.child = child;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getId() {
    return id;
  }

}

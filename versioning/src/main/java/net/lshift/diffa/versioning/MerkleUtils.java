package net.lshift.diffa.versioning;

public class MerkleUtils {

  public static MerkleNode buildEntityIdNode(String id, String version) {
    MerkleNode leaf = new MerkleNode(id.substring(4,6), id, version);
    MerkleNode mid = new MerkleNode(id.substring(2,4), leaf);
    return new MerkleNode(id.substring(0,2), mid);
  }
}
